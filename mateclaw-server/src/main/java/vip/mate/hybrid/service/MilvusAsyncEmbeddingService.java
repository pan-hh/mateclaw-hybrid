package vip.mate.hybrid.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import vip.mate.llm.embedding.EmbeddingModelFactory;
import vip.mate.llm.model.ModelConfigEntity;
import vip.mate.llm.service.ModelConfigService;
import vip.mate.hybrid.HybridStoreManager;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.wiki.model.WikiChunkEntity;
import vip.mate.wiki.repository.WikiChunkMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 异步向量化服务。
 * <p>
 * 将 Wiki 知识库的 Chunk 内容批量向量化后存储到向量数据库（Milvus/ES）。
 * 使用 {@code @Async("embeddingThreadPool")} 线程池实现非阻塞处理。
 * <p>
 * 单 chunk 嵌入失败时最多重试 3 次（指数退避），完全失败则记录到 {@link EmbeddingRetryService}。
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MilvusAsyncEmbeddingService {

    private final HybridStoreManager HybridStoreManager;
    private final EmbeddingModelFactory embeddingModelFactory;
    private final ModelConfigService modelConfigService;
    private final WikiChunkMapper wikiChunkMapper;
    private final EmbeddingRetryService embeddingRetryService;

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 5_000;

    /**
     * 异步批量嵌入并存储指定知识库的所有 chunk。
     *
     * @param kbId        知识库 ID
     * @param workspaceId 工作空间 ID
     */
    @Async("embeddingThreadPool")
    public void embedAndStoreAsync(Long kbId, Long workspaceId) {
        log.info("[MilvusAsync] Starting async embedding for kbId={}, workspaceId={}", kbId, workspaceId);

        try {
            List<WikiChunkEntity> chunks = wikiChunkMapper.selectList(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<WikiChunkEntity>()
                            .eq(WikiChunkEntity::getKbId, kbId)
                            .eq(WikiChunkEntity::getDeleted, 0));

            if (chunks.isEmpty()) {
                log.info("[MilvusAsync] No chunks found for kbId={}", kbId);
                return;
            }

            ModelConfigEntity modelConfig = modelConfigService.findFirstEnabledEmbedding();
            if (modelConfig == null) {
                log.error("[MilvusAsync] No embedding model available");
                return;
            }

            EmbeddingModel embeddingModel = embeddingModelFactory.build(modelConfig);

            List<HybridStoreProvider.VectorDocument> documents = chunks.stream()
                    .map(chunk -> {
                        try {
                            float[] embedding = embeddingWithRetry(chunk.getContent(), chunk.getId(),
                                    embeddingModel);
                            return new HybridStoreProvider.VectorDocument(
                                    chunk.getId().toString(),
                                    chunk.getContent(),
                                    embedding,
                                    Map.of("chunkId", chunk.getId(), "kbId", kbId,
                                            "rawId", chunk.getRawId() != null ? chunk.getRawId() : ""));
                        } catch (EmbeddingRetryException e) {
                            log.error("[MilvusAsync] Failed to embed chunk {} after {} retries",
                                    chunk.getId(), e.getRetryCount());
                            embeddingRetryService.recordFailedChunk(chunk.getId(), kbId, workspaceId);
                            return null;
                        } catch (Exception e) {
                            log.warn("[MilvusAsync] Failed to embed chunk {}: {}", chunk.getId(), e.getMessage());
                            embeddingRetryService.recordFailedChunk(chunk.getId(), kbId, workspaceId);
                            return null;
                        }
                    })
                    .filter(doc -> doc != null)
                    .collect(Collectors.toList());

            if (documents.isEmpty()) {
                log.warn("[MilvusAsync] All chunks failed to embed for kbId={}", kbId);
                return;
            }

            writeWithRetry(workspaceId, documents, kbId);
            log.info("[MilvusAsync] Completed for kbId={}", kbId);

        } catch (Exception e) {
            log.error("[MilvusAsync] Unexpected error for kbId={}: {}", kbId, e.getMessage(), e);
        }
    }

    /**
     * 同步检索（代理到 HybridStoreManager）。
     */
    public HybridStoreProvider.RetrievalResult retrieve(Long workspaceId, String query, int topK) {
        return HybridStoreManager.retrieve(workspaceId, query, topK);
    }

    // ---- 内部方法 ----

    private float[] embeddingWithRetry(String content, Long chunkId,
                                        EmbeddingModel embeddingModel) throws EmbeddingRetryException {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return embeddingModel.embed(content);
            } catch (Exception e) {
                lastException = e;
                log.warn("[MilvusAsync] Embedding attempt {} failed for chunk {}: {}",
                        attempt, chunkId, e.getMessage());

                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new EmbeddingRetryException(attempt, ie);
                    }
                }
            }
        }

        throw new EmbeddingRetryException(MAX_RETRY_ATTEMPTS, lastException);
    }

    private void writeWithRetry(Long workspaceId,
                                 List<HybridStoreProvider.VectorDocument> documents,
                                 Long kbId) {
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                HybridStoreProvider provider = HybridStoreManager.getActiveProvider();
                if (provider == null) {
                    log.error("[MilvusAsync] No active provider available");
                    embeddingRetryService.recordFailedDocuments(kbId, workspaceId,
                            documents.stream().map(HybridStoreProvider.VectorDocument::id).toList());
                    return;
                }

                var result = provider.writeAsync(workspaceId, documents).get();
                log.info("[MilvusAsync] Write completed: {}/{} succeeded",
                        result.successCount(), documents.size());

                if (result.failedCount() > 0) {
                    log.warn("[MilvusAsync] {} documents failed to write, scheduling retry",
                            result.failedCount());
                    embeddingRetryService.recordFailedDocuments(kbId, workspaceId, result.failedIds());
                }
                return;

            } catch (Exception e) {
                log.warn("[MilvusAsync] Write attempt {} failed: {}", attempt, e.getMessage());
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.error("[MilvusAsync] Write failed after {} attempts, recording all as failed", MAX_RETRY_ATTEMPTS);
        embeddingRetryService.recordFailedDocuments(kbId, workspaceId,
                documents.stream().map(HybridStoreProvider.VectorDocument::id).toList());
    }

    // ---- 内部异常类 ----

    public static class EmbeddingRetryException extends Exception {
        private final int retryCount;

        public EmbeddingRetryException(int retryCount, Throwable cause) {
            super("Failed after " + retryCount + " attempts", cause);
            this.retryCount = retryCount;
        }

        public int getRetryCount() {
            return retryCount;
        }
    }
}
