package vip.mate.hybrid.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import vip.mate.hybrid.HybridStoreManager;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.wiki.model.WikiPageEntity;
import vip.mate.wiki.repository.WikiPageMapper;

import java.util.List;
import java.util.Map;

/**
 * Elasticsearch 同步服务。
 * <p>
 * 将 Wiki 页面内容异步同步到 Elasticsearch 索引，供全文检索使用。
 * 同时提供同步全文检索方法。
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ElasticsearchSyncService {

    private final HybridStoreManager HybridStoreManager;
    private final WikiPageMapper wikiPageMapper;

    /**
     * 异步将知识库下所有页面同步到 ES。
     *
     * @param kbId        知识库 ID
     * @param workspaceId 工作空间 ID（用于 ES 索引隔离）
     */
    @Async("embeddingThreadPool")
    public void syncPageToEs(Long kbId, Long workspaceId) {
        log.info("[ESSync] Syncing pages for kbId={}, workspaceId={}", kbId, workspaceId);

        try {
            List<WikiPageEntity> pages = wikiPageMapper.selectList(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<WikiPageEntity>()
                            .eq(WikiPageEntity::getKbId, kbId)
                            .eq(WikiPageEntity::getDeleted, 0)
                            .eq(WikiPageEntity::getArchived, 0));

            if (pages.isEmpty()) {
                log.info("[ESSync] No pages found for kbId={}", kbId);
                return;
            }

            HybridStoreProvider esProvider = HybridStoreManager.getEsProvider();
            if (esProvider == null || !esProvider.isAvailable()) {
                log.warn("[ESSync] Elasticsearch not available, skipping sync");
                return;
            }

            List<HybridStoreProvider.VectorDocument> documents = pages.stream()
                    .map(page -> new HybridStoreProvider.VectorDocument(
                            page.getId().toString(),
                            page.getContent() != null ? page.getContent() : "",
                            null, // ES 不需要向量
                            Map.of("title", page.getTitle() != null ? page.getTitle() : "",
                                    "summary", page.getSummary() != null ? page.getSummary() : "",
                                    "kbId", kbId,
                                    "slug", page.getSlug() != null ? page.getSlug() : "")))
                    .toList();

            esProvider.writeAsync(workspaceId, documents)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("[ESSync] Sync failed for kbId={}: {}", kbId, error.getMessage());
                        } else {
                            log.info("[ESSync] Completed for kbId={}: {}/{} succeeded",
                                    kbId, result.successCount(), documents.size());
                        }
                    });

        } catch (Exception e) {
            log.error("[ESSync] Failed for kbId={}: {}", kbId, e.getMessage());
        }
    }

    /**
     * 异步同步单个页面到 ES。
     */
    @Async("embeddingThreadPool")
    public void syncSinglePageToEs(Long pageId, Long workspaceId) {
        WikiPageEntity page = wikiPageMapper.selectById(pageId);
        if (page == null || page.getDeleted() != null && page.getDeleted() == 1) {
            return;
        }
        syncPageToEs(page.getKbId(), workspaceId);
    }

    /**
     * ES 全文检索（同步）。
     *
     * @param workspaceId 工作空间 ID
     * @param query       查询文本
     * @param topK        返回结果数
     */
    public HybridStoreProvider.RetrievalResult search(Long workspaceId, String query, int topK) {
        HybridStoreProvider esProvider = HybridStoreManager.getEsProvider();
        if (esProvider == null || !esProvider.isAvailable()) {
            return new HybridStoreProvider.RetrievalResult(List.of(), false, "ES not available");
        }

        try {
            return esProvider.retrieveAsync(workspaceId, query, topK).get();
        } catch (Exception e) {
            log.error("[ESSync] Search failed for workspaceId={}: {}", workspaceId, e.getMessage());
            return new HybridStoreProvider.RetrievalResult(List.of(), false, e.getMessage());
        }
    }
}
