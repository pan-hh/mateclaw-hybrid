package vip.mate.wiki.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import vip.mate.memory.service.MemoryRecallService;
import vip.mate.hybrid.provider.FallbackHybridStoreProvider;
import vip.mate.hybrid.service.ElasticsearchSyncService;
import vip.mate.hybrid.service.MilvusAsyncEmbeddingService;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.hybrid.util.CircuitBreaker;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 混合检索器。
 * <p>
 * 核心能力：
 * <ul>
 *   <li>并行检索：ES 全文检索 + Milvus 向量检索（CompletableFuture 并发）</li>
 *   <li>RRF (Reciprocal Rank Fusion) 融合排序：score = Σ 1/(k + rank_i)，k=60</li>
 *   <li>三级熔断器：ES / Milvus / Hybrid 各自独立熔断</li>
 *   <li>完整降级链路：ES + Milvus → 单路 → MySQL 向量 → MySQL 文本</li>
 *   <li>检索结果去重：基于 content 前 100 字符 hashCode</li>
 * </ul>
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HybridRetriever {

    private final ElasticsearchSyncService esService;
    private final MilvusAsyncEmbeddingService milvusService;
    private final FallbackHybridStoreProvider fallbackProvider;
    private final MemoryRecallService memoryRecallService;

    /** 各组件独立熔断器 */
    private final CircuitBreaker esCircuitBreaker = new CircuitBreaker("ES", 3, 60_000);
    private final CircuitBreaker milvusCircuitBreaker = new CircuitBreaker("Milvus", 3, 60_000);
    private final CircuitBreaker hybridCircuitBreaker = new CircuitBreaker("Hybrid", 3, 60_000);

    /** RRF 参数 k */
    private static final int RRF_K = 60;

    /** 并行检索超时 */
    private static final long SEARCH_TIMEOUT_SECONDS = 30;

    public enum RetrievalMode {
        /** 仅 Elasticsearch 全文检索 */
        ES_ONLY,
        /** 仅 Milvus 向量检索 */
        VECTOR_ONLY,
        /** 混合检索（融合 ES + Milvus 结果） */
        HYBRID
    }

    /**
     * 执行混合检索。
     *
     * @param workspaceId 工作空间 ID
     * @param query       查询文本
     * @param topK        返回结果数
     * @param mode        检索模式
     * @return 混合检索结果
     */
    public HybridResult retrieve(Long workspaceId, String query, int topK, RetrievalMode mode) {
        log.info("[HybridRetriever] Mode={}, ES state={}, Milvus state={}",
                mode, esCircuitBreaker.getState(), milvusCircuitBreaker.getState());

        // 全链路熔断 → 直接降级到 MySQL
        if (hybridCircuitBreaker.shouldFallback()) {
            log.warn("[HybridRetriever] Hybrid circuit breaker OPEN, using full fallback");
            List<SearchHit> fallbackHits = executeFullFallback(workspaceId, query, topK);
            return new HybridResult(fallbackHits, 0, 0, true);
        }

        // 并行触发 ES + Milvus
        List<SearchHit> esHits = List.of();
        List<SearchHit> vectorHits = List.of();
        boolean esSuccess = false;
        boolean milvusSuccess = false;
        boolean fallbackUsed = false;

        CompletableFuture<List<SearchHit>> esFuture = CompletableFuture.supplyAsync(() -> {
            if (mode == RetrievalMode.VECTOR_ONLY) return List.of();

            if (esCircuitBreaker.shouldFallback()) {
                log.warn("[HybridRetriever] ES circuit breaker OPEN, skipping ES");
                return List.<SearchHit>of();
            }

            try {
                HybridStoreProvider.RetrievalResult result =
                        esService.search(workspaceId, query, topK * 3);
                if (result.success()) {
                    esCircuitBreaker.recordSuccess();
                    return convertEsHits(result);
                } else {
                    esCircuitBreaker.recordFailure();
                    return List.<SearchHit>of();
                }
            } catch (Exception e) {
                esCircuitBreaker.recordFailure();
                log.error("[HybridRetriever] ES search failed: {}", e.getMessage());
                return List.<SearchHit>of();
            }
        }).exceptionally(ex -> {
            esCircuitBreaker.recordFailure();
            return List.of();
        });

        CompletableFuture<List<SearchHit>> vectorFuture = CompletableFuture.supplyAsync(() -> {
            if (mode == RetrievalMode.ES_ONLY) return List.of();

            if (milvusCircuitBreaker.shouldFallback()) {
                log.warn("[HybridRetriever] Milvus circuit breaker OPEN, using MySQL vector fallback");
                return executeMysqlVectorSearch(workspaceId, query, topK * 3);
            }

            try {
                HybridStoreProvider.RetrievalResult result =
                        milvusService.retrieve(workspaceId, query, topK * 3);
                if (result.success()) {
                    milvusCircuitBreaker.recordSuccess();
                    return convertVectorHits(result);
                } else {
                    milvusCircuitBreaker.recordFailure();
                    log.warn("[HybridRetriever] Milvus failed, using MySQL vector fallback");
                    return executeMysqlVectorSearch(workspaceId, query, topK * 3);
                }
            } catch (Exception e) {
                milvusCircuitBreaker.recordFailure();
                log.warn("[HybridRetriever] Milvus error, using MySQL vector fallback: {}", e.getMessage());
                return executeMysqlVectorSearch(workspaceId, query, topK * 3);
            }
        }).exceptionally(ex -> {
            milvusCircuitBreaker.recordFailure();
            return executeMysqlVectorSearch(workspaceId, query, topK * 3);
        });

        try {
            CompletableFuture.allOf(esFuture, vectorFuture)
                    .get(SEARCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            esHits = esFuture.get();
            vectorHits = vectorFuture.get();
        } catch (Exception e) {
            log.error("[HybridRetriever] Timeout or error: {}", e.getMessage());
        }

        // 检查是否使用了 MySQL 向量降级
        fallbackUsed = vectorHits.stream()
                .anyMatch(h -> "MYSQL_VECTOR".equals(h.source()))
                || vectorHits.stream().anyMatch(h -> "FALLBACK".equals(h.source()));

        // 根据成功情况判断
        esSuccess = !esHits.isEmpty() || mode == RetrievalMode.VECTOR_ONLY;
        milvusSuccess = !vectorHits.isEmpty() || mode == RetrievalMode.ES_ONLY;

        // 两者都失败 → 全量降级到 MySQL
        if (!esSuccess && !milvusSuccess) {
            hybridCircuitBreaker.recordFailure();
            log.warn("[HybridRetriever] Both engines failed, using full fallback");
            List<SearchHit> fallbackHits = executeFullFallback(workspaceId, query, topK);
            return new HybridResult(fallbackHits, 0, 0, true);
        }

        // 至少一路成功 → 重置 Hybrid 熔断器
        if (esSuccess || milvusSuccess) {
            hybridCircuitBreaker.recordSuccess();
        }

        List<SearchHit> mergedHits = switch (mode) {
            case ES_ONLY -> esHits;
            case VECTOR_ONLY -> vectorHits;
            case HYBRID -> mergeAndRerank(esHits, vectorHits, topK);
        };

        return new HybridResult(mergedHits, esHits.size(), vectorHits.size(), fallbackUsed);
    }

    /**
     * 执行混合检索并记录 Chunk 召回。
     * <p>
     * 在检索成功后，遍历结果中包含 {@code chunkId} 元数据的命中，
     * 调用 {@link MemoryRecallService#recordChunkRecall} 记录精准的 Chunk 级别召回频率。
     *
     * @param workspaceId 工作空间 ID
     * @param agentId     Agent ID（用于记忆召回追踪，null 则跳过记录）
     * @param query       查询文本
     * @param topK        返回结果数
     * @param mode        检索模式
     * @return 混合检索结果
     */
    public HybridResult retrieveAndRecord(Long workspaceId, Long agentId, String query,
                                           int topK, RetrievalMode mode) {
        HybridResult result = retrieve(workspaceId, query, topK, mode);

        if (agentId != null && result.hits() != null && !result.hits().isEmpty()) {
            String queryHash = generateQueryHash(query);
            for (SearchHit hit : result.hits()) {
                if (hit.metadata() != null && hit.metadata().containsKey("chunkId")) {
                    try {
                        Long chunkId = Long.parseLong(String.valueOf(hit.metadata().get("chunkId")));
                        Long pageId = hit.metadata().containsKey("pageId")
                                ? Long.parseLong(String.valueOf(hit.metadata().get("pageId")))
                                : null;
                        String filename = hit.metadata().containsKey("filename")
                                ? String.valueOf(hit.metadata().get("filename"))
                                : null;

                        memoryRecallService.recordChunkRecall(
                                agentId, chunkId, hit.content(), queryHash, pageId, filename);
                    } catch (NumberFormatException e) {
                        log.debug("[HybridRetriever] Cannot parse chunkId from metadata: {}",
                                hit.metadata().get("chunkId"));
                    }
                }
            }
        }

        return result;
    }

    /**
     * 生成查询文本的简短哈希（用于召回多样性计算）。
     */

    private List<SearchHit> convertEsHits(HybridStoreProvider.RetrievalResult result) {
        return result.hits().stream()
                .map(hit -> new SearchHit(
                        "es:" + hit.id(),
                        hit.metadata() != null ? String.valueOf(hit.metadata().getOrDefault("title", "")) : null,
                        hit.content(),
                        hit.score(),
                        "ES",
                        hit.metadata()))
                .collect(Collectors.toList());
    }

    // ---- 向量结果转换（Milvus + MySQL 向量） ----

    private List<SearchHit> convertVectorHits(HybridStoreProvider.RetrievalResult result) {
        return result.hits().stream()
                .map(hit -> new SearchHit(
                        "vector:" + hit.id(),
                        hit.metadata() != null ? String.valueOf(hit.metadata().getOrDefault("pageTitle", "")) : null,
                        hit.content(),
                        hit.score(),
                        result.fallbackUsed() ? "MYSQL_VECTOR" : "VECTOR",
                        hit.metadata()))
                .collect(Collectors.toList());
    }

    // ---- MySQL 向量降级 ----

    private List<SearchHit> executeMysqlVectorSearch(Long workspaceId, String query, int topK) {
        try {
            var result = fallbackProvider.retrieveAsync(workspaceId, query, topK).get();
            return result.hits().stream()
                    .map(hit -> new SearchHit(
                            "mysql-vector:" + hit.id(),
                            null,
                            hit.content(),
                            hit.score(),
                            "MYSQL_VECTOR",
                            hit.metadata()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[HybridRetriever] MySQL vector search failed: {}", e.getMessage());
            return List.of();
        }
    }

    // ---- 全量降级（ES + Milvus 双失败） ----

    private List<SearchHit> executeFullFallback(Long workspaceId, String query, int topK) {
        try {
            var result = fallbackProvider.retrieveAsync(workspaceId, query, topK).get();
            return result.hits().stream()
                    .map(hit -> new SearchHit(
                            "fallback:" + hit.id(),
                            null,
                            hit.content(),
                            hit.score(),
                            "FALLBACK",
                            hit.metadata()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[HybridRetriever] Full fallback failed: {}", e.getMessage());
            return List.of();
        }
    }

    // ---- RRF 融合排序 ----

    /**
     * RRF (Reciprocal Rank Fusion) 融合算法。
     * score = Σ 1/(k + rank_i)
     *
     * @param esHits     ES 检索结果
     * @param vectorHits 向量检索结果
     * @param topK       最终返回数
     * @return 融合排序后的结果
     */
    private List<SearchHit> mergeAndRerank(List<SearchHit> esHits,
                                            List<SearchHit> vectorHits, int topK) {
        Map<String, Double> fusedScores = new LinkedHashMap<>();
        Map<String, SearchHit> hitMap = new LinkedHashMap<>();

        // ES 结果排名
        for (int i = 0; i < esHits.size(); i++) {
            SearchHit hit = esHits.get(i);
            String key = generateDedupKey(hit);
            fusedScores.merge(key, 1.0 / (RRF_K + i + 1), Double::sum);
            hitMap.putIfAbsent(key, hit);
        }

        // 向量结果排名（包含 MySQL 向量）
        for (int i = 0; i < vectorHits.size(); i++) {
            SearchHit hit = vectorHits.get(i);
            String key = generateDedupKey(hit);
            fusedScores.merge(key, 1.0 / (RRF_K + i + 1), Double::sum);
            hitMap.putIfAbsent(key, hit);
        }

        // 重新构建结果
        return fusedScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(topK)
                .map(entry -> {
                    SearchHit original = hitMap.get(entry.getKey());
                    return new SearchHit(
                            original.id(),
                            original.title(),
                            original.content(),
                            entry.getValue().floatValue(),
                            "HYBRID",
                            original.metadata());
                })
                .collect(Collectors.toList());
    }

    /**
     * 生成去重键：基于 content 前 100 字符的 hashCode。
     */
    private String generateDedupKey(SearchHit hit) {
        String content = hit.content() != null ? hit.content() : "";
        String trimmed = content.length() > 100 ? content.substring(0, 100) : content;
        return String.valueOf(trimmed.hashCode());
    }

    /**
     * 生成查询哈希。
     */
    private String generateQueryHash(String query) {
        if (query == null) return "";
        return Integer.toHexString(query.hashCode());
    }

    // ---- 数据记录类型 ----

    /**
     * 单条检索命中结果。
     */
    public record SearchHit(
            String id,
            String title,
            String content,
            float score,
            String source,     // "ES" / "VECTOR" / "MYSQL_VECTOR" / "FALLBACK" / "HYBRID"
            Map<String, Object> metadata) {
    }

    /**
     * 混合检索结果。
     */
    public record HybridResult(
            List<SearchHit> hits,
            int esHitCount,
            int vectorHitCount,
            boolean vectorFallbackUsed) {
    }
}
