package vip.mate.hybrid.provider;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MultiMatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;
import vip.mate.hybrid.config.ElasticsearchProperties;
import vip.mate.hybrid.spi.HybridStoreProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Elasticsearch 全文检索提供者。
 * <p>
 * 基于 BM25 算法提供关键词全文检索能力，支持中文 IK 分词器。
 * 每个工作空间使用独立索引 {@code {indexPrefix}_{workspaceId}} 实现多租户隔离。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ElasticsearchProvider implements HybridStoreProvider {

    private final ElasticsearchProperties properties;
    private ElasticsearchClient client;

    @PostConstruct
    void init() {
        if (!properties.isEnabled()) {
            log.info("[ES] Elasticsearch is disabled via configuration");
            return;
        }
        try {
            RestClient restClient = RestClient.builder(
                            new HttpHost(properties.getHost(), properties.getPort(), properties.getScheme()))
                    .setRequestConfigCallback(rc -> rc
                            .setConnectTimeout(properties.getConnectTimeout())
                            .setSocketTimeout(properties.getSocketTimeout()))
                    .build();

            this.client = new ElasticsearchClient(new JacksonJsonpMapper(),
                    new RestClientTransport(restClient, new JacksonJsonpMapper()));

            log.info("[ES] Connected to Elasticsearch {}:{}", properties.getHost(), properties.getPort());
        } catch (Exception e) {
            log.error("[ES] Failed to connect to Elasticsearch: {}", e.getMessage());
            this.client = null;
        }
    }

    @Override
    public String id() {
        return "elasticsearch";
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public boolean isAvailable() {
        return properties.isEnabled() && client != null;
    }

    @Override
    public CompletableFuture<WriteResult> writeAsync(Long workspaceId, List<VectorDocument> documents) {
        return CompletableFuture.supplyAsync(() -> {
            if (!isAvailable()) {
                log.warn("[ES] Not available, skipping write for workspaceId={}", workspaceId);
                return new WriteResult(0, documents.size(),
                        documents.stream().map(VectorDocument::id).toList());
            }

            String indexName = buildIndexName(workspaceId);
            ensureIndexExists(indexName);

            int successCount = 0;
            List<String> failedIds = new ArrayList<>();

            for (VectorDocument doc : documents) {
                try {
                    Map<String, Object> source = new HashMap<>();
                    source.put("id", doc.id());
                    source.put("content", doc.content());
                    source.put("title", doc.metadata() != null ? doc.metadata().getOrDefault("title", "") : "");
                    source.put("summary", doc.metadata() != null ? doc.metadata().getOrDefault("summary", "") : "");
                    source.put("metadata", doc.metadata() != null ? doc.metadata() : Map.of());

                    IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
                            .index(indexName)
                            .id(doc.id())
                            .document(source));

                    client.index(request);
                    successCount++;
                } catch (Exception e) {
                    log.error("[ES] Index failed for doc {}: {}", doc.id(), e.getMessage());
                    failedIds.add(doc.id());
                }
            }

            return new WriteResult(successCount, failedIds.size(), failedIds);
        });
    }

    @Override
    public CompletableFuture<RetrievalResult> retrieveAsync(Long workspaceId, String query, int topK) {
        return CompletableFuture.supplyAsync(() -> {
            if (!isAvailable()) {
                return new RetrievalResult(Collections.emptyList(), false, "ES not available");
            }

            String indexName = buildIndexName(workspaceId);

            try {
                Query queryBuilder = MultiMatchQuery.of(m -> m
                                .query(query)
                                .fields("content^3", "title^2", "summary")
                                .fuzziness("AUTO"))
                        ._toQuery();

                SearchRequest request = SearchRequest.of(s -> s
                        .index(indexName)
                        .query(queryBuilder)
                        .size(topK));

                @SuppressWarnings("rawtypes")
                SearchResponse<Map> response = client.search(request, Map.class);

                List<RetrievalHit> hits = new ArrayList<>();
                for (Hit<Map> hit : response.hits().hits()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source = hit.source();
                    if (source != null) {
                        hits.add(new RetrievalHit(
                                hit.id(),
                                source.getOrDefault("content", "").toString(),
                                hit.score() != null ? hit.score().floatValue() : 0f,
                                safeGetMetadata(source)));
                    }
                }

                return new RetrievalResult(hits, false, null);

            } catch (Exception e) {
                log.error("[ES] Search failed for workspaceId={}: {}", workspaceId, e.getMessage());
                return new RetrievalResult(Collections.emptyList(), false, e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Long workspaceId, List<String> documentIds) {
        return CompletableFuture.runAsync(() -> {
            if (!isAvailable()) return;
            String indexName = buildIndexName(workspaceId);
            for (String id : documentIds) {
                try {
                    client.delete(d -> d.index(indexName).id(id));
                } catch (Exception e) {
                    log.error("[ES] Delete failed for {}: {}", id, e.getMessage());
                }
            }
        });
    }

    @Override
    public CompletableFuture<Void> clearAsync(Long workspaceId) {
        return CompletableFuture.runAsync(() -> {
            if (!isAvailable()) return;
            try {
                client.indices().delete(d -> d.index(buildIndexName(workspaceId)));
                log.info("[ES] Cleared index for workspaceId={}", workspaceId);
            } catch (Exception e) {
                log.error("[ES] Clear failed for workspaceId={}: {}", workspaceId, e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<VectorStats> getStatsAsync(Long workspaceId) {
        return CompletableFuture.completedFuture(new VectorStats(0, 0, "ELASTICSEARCH"));
    }

    // ---- 内部方法 ----

    private String buildIndexName(Long workspaceId) {
        return properties.getIndexPrefix() + "_" + workspaceId;
    }

    private void ensureIndexExists(String indexName) {
        try {
            boolean exists = client.indices().exists(e -> e.index(indexName)).value();
            if (!exists) {
                client.indices().create(c -> c.index(indexName).mappings(m -> m
                        .properties("content", p -> p.text(t -> t.analyzer("ik_max_word")))
                        .properties("title", p -> p.text(t -> t.analyzer("ik_max_word")))
                        .properties("summary", p -> p.text(t -> t.analyzer("ik_max_word")))));
                log.info("[ES] Created index: {}", indexName);
            }
        } catch (Exception e) {
            log.error("[ES] Failed to create index {}: {}", indexName, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> safeGetMetadata(Map<String, Object> source) {
        Object meta = source.get("metadata");
        if (meta instanceof Map) {
            return (Map<String, Object>) meta;
        }
        return Map.of();
    }
}
