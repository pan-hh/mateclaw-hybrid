package vip.mate.hybrid.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vip.mate.hybrid.HybridStoreManager;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.wiki.model.WikiChunkEntity;
import vip.mate.wiki.model.WikiPageEntity;
import vip.mate.wiki.repository.WikiChunkMapper;
import vip.mate.wiki.repository.WikiPageMapper;

import java.util.List;

/**
 * 数据一致性保障服务。
 * <p>
 * 保证 Wiki 文档在 MySQL / Elasticsearch / Milvus 三者之间的数据一致性。
 * 核心流程：
 * <ol>
 *   <li>保存文档：MySQL 写入 → ES 异步同步 → Milvus 异步向量化</li>
 *   <li>删除文档：ES 删除 → Milvus 删除 → MySQL 删除</li>
 *   <li>全量同步：按 KB 全量重新索引</li>
 * </ol>
 * <p>
 * 使用 Spring {@code @Transactional} + {@code @Retryable} 保障事务一致性，
 * 失败时通过补偿任务调度处理。
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataConsistencyService {

    private final WikiPageMapper wikiPageMapper;
    private final WikiChunkMapper wikiChunkMapper;
    private final HybridStoreManager HybridStoreManager;
    private final DocumentChunkService documentChunkService;
    private final MilvusAsyncEmbeddingService milvusAsyncEmbeddingService;
    private final ElasticsearchSyncService elasticsearchSyncService;

    /**
     * 保存文档并同步到 ES/Milvus。
     * <p>
     * 流程：MySQL 写入 page + chunks → ES 异步同步 → Milvus 异步向量化。
     * 事务内完成 MySQL 写入，事务外异步触发 ES/Milvus。
     *
     * @param workspaceId 工作空间 ID
     * @param page        待保存的 Wiki 页面
     */
    @Transactional(rollbackFor = Exception.class)
    @Retryable(
            retryFor = {Exception.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2))
    public void saveDocumentWithConsistency(Long workspaceId, WikiPageEntity page) {
        log.info("[Consistency] Saving document: workspaceId={}, pageId={}", workspaceId, page.getId());

        try {
            // 1. MySQL 写入
            wikiPageMapper.insert(page);

            // 2. 切分 + 写入 chunks
            List<WikiChunkEntity> chunks = documentChunkService.chunkDocument(
                    page.getId(), page.getKbId(), page.getContent());
            documentChunkService.saveChunks(chunks);

            log.info("[Consistency] Page saved, {} chunks created", chunks.size());

            // 3. 异步触发 ES 同步（不阻塞事务返回）
            try {
                elasticsearchSyncService.syncPageToEs(page.getKbId(), workspaceId);
            } catch (Exception e) {
                log.warn("[Consistency] ES sync trigger failed (non-fatal): {}", e.getMessage());
            }

            // 4. 异步触发 Milvus 向量化（不阻塞事务返回）
            try {
                milvusAsyncEmbeddingService.embedAndStoreAsync(page.getKbId(), workspaceId);
            } catch (Exception e) {
                log.warn("[Consistency] Milvus embedding trigger failed (non-fatal): {}", e.getMessage());
            }

            log.info("[Consistency] Document saved successfully: pageId={}", page.getId());

        } catch (Exception e) {
            log.error("[Consistency] Failed to save document: {}", e.getMessage());
            throw new RuntimeException("Document save failed: " + e.getMessage(), e);
        }
    }

    /**
     * 删除文档并同步清理 ES/Milvus。
     * <p>
     * 流程：ES 删除 → Milvus 删除 → MySQL chunk/page 删除。
     *
     * @param workspaceId 工作空间 ID
     * @param pageId      页面 ID
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteDocumentWithConsistency(Long workspaceId, Long pageId) {
        WikiPageEntity page = wikiPageMapper.selectById(pageId);
        if (page == null || (page.getDeleted() != null && page.getDeleted() == 1)) {
            return;
        }

        log.info("[Consistency] Deleting document: workspaceId={}, pageId={}", workspaceId, pageId);

        try {
            HybridStoreProvider milvusProvider = HybridStoreManager.getActiveProvider();
            HybridStoreProvider esProvider = HybridStoreManager.getEsProvider();

            // 1. 获取关联的 chunk IDs
            List<WikiChunkEntity> chunks = wikiChunkMapper.selectList(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<WikiChunkEntity>()
                            .eq(WikiChunkEntity::getRawId, pageId));
            List<String> chunkIds = chunks.stream()
                    .map(c -> c.getId().toString())
                    .toList();

            // 2. 删除 ES 索引
            if (esProvider != null && esProvider.isAvailable()) {
                try {
                    esProvider.deleteAsync(workspaceId, List.of(pageId.toString()));
                } catch (Exception e) {
                    log.warn("[Consistency] ES delete failed (non-fatal): {}", e.getMessage());
                }
            }

            // 3. 删除 Milvus 向量
            if (milvusProvider != null && milvusProvider.isAvailable() && !chunkIds.isEmpty()) {
                try {
                    milvusProvider.deleteAsync(workspaceId, chunkIds);
                } catch (Exception e) {
                    log.warn("[Consistency] Milvus delete failed (non-fatal): {}", e.getMessage());
                }
            }

            // 4. 删除 MySQL chunks
            wikiChunkMapper.delete(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<WikiChunkEntity>()
                            .eq(WikiChunkEntity::getRawId, pageId));

            // 5. 软删除 MySQL page（保留历史）
            page.setDeleted(1);
            wikiPageMapper.updateById(page);

            log.info("[Consistency] Document deleted: pageId={}", pageId);

        } catch (Exception e) {
            log.error("[Consistency] Delete failed for pageId={}: {}", pageId, e.getMessage());
            scheduleCompensationTask(workspaceId, pageId, "DELETE");
        }
    }

    /**
     * 全量同步指定知识库的所有文档到 ES/Milvus。
     *
     * @param workspaceId 工作空间 ID
     * @param kbId        知识库 ID
     */
    public void syncAllDocuments(Long workspaceId, Long kbId) {
        log.info("[Consistency] Syncing all documents for kbId={}", kbId);

        List<WikiPageEntity> pages = wikiPageMapper.selectList(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<WikiPageEntity>()
                        .eq(WikiPageEntity::getKbId, kbId)
                        .eq(WikiPageEntity::getDeleted, 0)
                        .eq(WikiPageEntity::getArchived, 0));

        for (WikiPageEntity page : pages) {
            try {
                elasticsearchSyncService.syncPageToEs(kbId, workspaceId);
                milvusAsyncEmbeddingService.embedAndStoreAsync(kbId, workspaceId);
            } catch (Exception e) {
                log.error("[Consistency] Sync failed for page {}: {}", page.getId(), e.getMessage());
            }
        }

        log.info("[Consistency] Sync completed for kbId={}, {} pages processed", kbId, pages.size());
    }

    /**
     * 补偿任务调度（留空，后续可对接 cron 或 MQ）。
     */
    private void scheduleCompensationTask(Long workspaceId, Long pageId, String operation) {
        log.warn("[Consistency] Scheduling compensation task: operation={}, workspaceId={}, pageId={}",
                operation, workspaceId, pageId);
        // 后续可对接定时任务或消息队列做补偿重试
    }
}
