package vip.mate.hybrid.provider;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.stereotype.Component;
import vip.mate.llm.embedding.EmbeddingModelFactory;
import vip.mate.llm.model.ModelConfigEntity;
import vip.mate.llm.service.ModelConfigService;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.wiki.model.WikiChunkEntity;
import vip.mate.wiki.model.WikiPageEntity;
import vip.mate.wiki.repository.WikiChunkMapper;
import vip.mate.wiki.repository.WikiPageMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * MySQL 降级向量存储提供者。
 * <p>
 * 当 Elasticsearch 和 Milvus 都不可用时，回退到 MySQL 进行检索。
 * 支持两级降级策略：
 * <ol>
 *   <li><b>向量检索</b>：从 {@code mate_wiki_chunk} 读取 embedding，计算余弦相似度排序</li>
 *   <li><b>文本检索</b>：通过 {@code mate_wiki_page} 的 LIKE 关键词匹配</li>
 * </ol>
 * <p>
 * 注意：当前接口使用 {@code workspaceId} 参数，Fallback 内部将其视为 {@code kbId}
 * （知识库 ID）进行查询。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FallbackHybridStoreProvider implements HybridStoreProvider {

    private final WikiChunkMapper wikiChunkMapper;
    private final WikiPageMapper wikiPageMapper;
    private final EmbeddingModelFactory embeddingModelFactory;
    private final ModelConfigService modelConfigService;

    @Override
    public String id() {
        return "fallback";
    }

    @Override
    public int order() {
        return 1000; // 最低优先级，最后选用
    }

    @Override
    public boolean isAvailable() {
        return true; // 始终可用，作为最终兜底
    }

    @Override
    public CompletableFuture<WriteResult> writeAsync(Long workspaceId, List<VectorDocument> documents) {
        return CompletableFuture.supplyAsync(() -> {
            if (documents.isEmpty()) {
                return new WriteResult(0, 0, List.of());
            }

            log.info("[Fallback] Writing {} documents to MySQL fallback store", documents.size());
            int successCount = 0;
            List<String> failedIds = new ArrayList<>();

            for (VectorDocument doc : documents) {
                try {
                    WikiChunkEntity chunk = new WikiChunkEntity();
                    chunk.setId(Long.parseLong(doc.id()));
                    chunk.setContent(doc.content());
                    chunk.setEmbedding(floatsToBytes(doc.embedding()));
                    chunk.setKbId(workspaceId);
                    chunk.setDeleted(0);
                    wikiChunkMapper.insert(chunk);
                    successCount++;
                } catch (Exception e) {
                    log.warn("[Fallback] Failed to write document {}: {}", doc.id(), e.getMessage());
                    failedIds.add(doc.id());
                }
            }

            return new WriteResult(successCount, failedIds.size(), failedIds);
        });
    }

    @Override
    public CompletableFuture<RetrievalResult> retrieveAsync(Long workspaceId, String query, int topK) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("[Fallback] Starting fallback retrieval for workspaceId/kbId={}", workspaceId);

            List<RetrievalHit> hits;
            String errorMessage = null;

            try {
                hits = tryVectorRetrieval(workspaceId, query, topK);
                log.info("[Fallback] Vector retrieval succeeded, found {} hits", hits.size());
            } catch (Exception e) {
                log.warn("[Fallback] Vector retrieval failed: {}, falling back to text search", e.getMessage());
                errorMessage = "Vector retrieval failed: " + e.getMessage();

                try {
                    hits = tryTextRetrieval(workspaceId, query, topK);
                    log.info("[Fallback] Text retrieval succeeded, found {} hits", hits.size());
                } catch (Exception textEx) {
                    log.error("[Fallback] Text retrieval also failed: {}", textEx.getMessage());
                    errorMessage += ", Text retrieval failed: " + textEx.getMessage();
                    hits = List.of();
                }
            }

            return new RetrievalResult(hits, true, hits.isEmpty() ? errorMessage : null);
        });
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Long workspaceId, List<String> documentIds) {
        return CompletableFuture.runAsync(() -> {
            log.info("[Fallback] Deleting {} documents", documentIds.size());
            for (String id : documentIds) {
                try {
                    wikiChunkMapper.deleteById(Long.parseLong(id));
                } catch (Exception e) {
                    log.warn("[Fallback] Failed to delete document {}: {}", id, e.getMessage());
                }
            }
        });
    }

    @Override
    public CompletableFuture<Void> clearAsync(Long workspaceId) {
        return CompletableFuture.runAsync(() -> {
            log.info("[Fallback] Clearing all documents for workspaceId/kbId={}", workspaceId);
            try {
                LambdaQueryWrapper<WikiChunkEntity> wrapper = new LambdaQueryWrapper<>();
                wrapper.eq(WikiChunkEntity::getKbId, workspaceId);
                wikiChunkMapper.delete(wrapper);
            } catch (Exception e) {
                log.error("[Fallback] Clear failed: {}", e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<VectorStats> getStatsAsync(Long workspaceId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                LambdaQueryWrapper<WikiChunkEntity> wrapper = new LambdaQueryWrapper<>();
                wrapper.eq(WikiChunkEntity::getKbId, workspaceId)
                        .eq(WikiChunkEntity::getDeleted, 0)
                        .isNotNull(WikiChunkEntity::getEmbedding);
                long count = wikiChunkMapper.selectCount(wrapper);
                return new VectorStats(count, 1024, "MYSQL_FALLBACK");
            } catch (Exception e) {
                log.warn("[Fallback] Get stats failed: {}", e.getMessage());
                return new VectorStats(0, 0, "MYSQL_FALLBACK");
            }
        });
    }

    // ---- 检索实现 ----

    /**
     * 基于 MySQL 中存储的 chunk embedding 进行余弦相似度检索。
     */
    private List<RetrievalHit> tryVectorRetrieval(Long workspaceId, String query, int topK) {
        EmbeddingModel embeddingModel = getEmbeddingModel();
        if (embeddingModel == null) {
            throw new RuntimeException("No embedding model available for fallback vector retrieval");
        }

        float[] queryEmbedding = embeddingModel.embed(query);

        // 查询该 kbId 下所有有 embedding 的 chunk
        LambdaQueryWrapper<WikiChunkEntity> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(WikiChunkEntity::getKbId, workspaceId)
                .eq(WikiChunkEntity::getDeleted, 0)
                .isNotNull(WikiChunkEntity::getEmbedding);
        List<WikiChunkEntity> allChunks = wikiChunkMapper.selectList(wrapper);

        if (allChunks.isEmpty()) {
            throw new RuntimeException("No vector data found in MySQL for workspaceId=" + workspaceId);
        }

        List<RetrievalHit> hits = new ArrayList<>();
        for (WikiChunkEntity chunk : allChunks) {
            try {
                if (chunk.getEmbedding() == null || chunk.getEmbedding().length == 0) {
                    continue;
                }
                float[] chunkEmbedding = bytesToFloats(chunk.getEmbedding());
                float similarity = cosineSimilarity(queryEmbedding, chunkEmbedding);

                hits.add(new RetrievalHit(
                        chunk.getId().toString(),
                        chunk.getContent(),
                        similarity,
                        Map.of("chunkId", chunk.getId(), "rawId",
                                chunk.getRawId() != null ? chunk.getRawId() : "")));
            } catch (Exception e) {
                log.debug("[Fallback] Failed to process chunk {}: {}", chunk.getId(), e.getMessage());
            }
        }

        hits.sort(Comparator.comparingDouble(RetrievalHit::score).reversed());
        return hits.stream().limit(topK).toList();
    }

    /**
     * 基于 MySQL LIKE 关键词匹配进行文本检索（降级的降级）。
     */
    private List<RetrievalHit> tryTextRetrieval(Long workspaceId, String query, int topK) {
        // 使用 WikiPageMapper 的关键词搜索（按 kbId 查询）
        String pattern = "%" + query.toLowerCase() + "%";
        List<WikiPageEntity> pages = wikiPageMapper.searchByKeyword(workspaceId, pattern);

        if (pages == null || pages.isEmpty()) {
            return List.of();
        }

        List<RetrievalHit> hits = new ArrayList<>();
        for (WikiPageEntity page : pages) {
            hits.add(new RetrievalHit(
                    page.getId().toString(),
                    page.getContent() != null ? page.getContent() : page.getSummary(),
                    0.1f, // LIKE 匹配无精确评分，给低分
                    Map.of("pageTitle", page.getTitle() != null ? page.getTitle() : "",
                            "kbId", page.getKbId() != null ? page.getKbId() : workspaceId,
                            "slug", page.getSlug() != null ? page.getSlug() : "")));
        }

        return hits.stream().limit(topK).toList();
    }

    // ---- 工具方法 ----

    private EmbeddingModel getEmbeddingModel() {
        try {
            ModelConfigEntity config = modelConfigService.findFirstEnabledEmbedding();
            if (config == null) {
                log.warn("[Fallback] No enabled embedding model config found");
                return null;
            }
            return embeddingModelFactory.build(config);
        } catch (Exception e) {
            log.error("[Fallback] Failed to build embedding model: {}", e.getMessage());
            return null;
        }
    }

    /**
     * float[] → byte[] (little-endian)。
     */
    private static byte[] floatsToBytes(float[] floats) {
        if (floats == null || floats.length == 0) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(floats.length * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        for (float f : floats) {
            buffer.putFloat(f);
        }
        return buffer.array();
    }

    /**
     * byte[] → float[] (little-endian)。
     */
    private static float[] bytesToFloats(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new float[0];
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] floats = new float[bytes.length / Float.BYTES];
        buffer.asFloatBuffer().get(floats);
        return floats;
    }

    /**
     * 计算两个向量的余弦相似度。
     */
    private static float cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length || a.length == 0) {
            return 0f;
        }
        double dotProduct = 0;
        double normA = 0;
        double normB = 0;
        for (int i = 0; i < a.length; i++) {
            dotProduct += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0 || normB == 0) {
            return 0f;
        }
        return (float) (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
    }
}
