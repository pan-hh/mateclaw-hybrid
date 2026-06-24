package vip.mate.hybrid.provider;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MetricType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.*;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.dml.*;
import io.milvus.grpc.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.stereotype.Component;
import vip.mate.llm.embedding.EmbeddingModelFactory;
import vip.mate.llm.model.ModelConfigEntity;
import vip.mate.llm.service.ModelConfigService;
import vip.mate.hybrid.config.MilvusProperties;
import vip.mate.hybrid.spi.HybridStoreProvider;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Milvus 向量检索提供者。
 * <p>
 * 基于 HNSW 索引 + 余弦相似度实现语义向量检索。
 * 每个工作空间使用独立 Collection {@code {prefix}_{workspaceId}_vectors} 实现多租户隔离。
 * <p>
 * Collection 缓存：按 workspaceId 缓存，避免重复创建。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MilvusHybridStoreProvider implements HybridStoreProvider {

    private final MilvusProperties properties;
    private final EmbeddingModelFactory embeddingModelFactory;
    private final ModelConfigService modelConfigService;

    private MilvusServiceClient milvusClient;

    /** 已初始化的 collection 名称集合（线程安全） */
    private final Set<String> initializedCollections = ConcurrentHashMap.newKeySet();

    @PostConstruct
    void init() {
        if (!properties.isEnabled()) {
            log.info("[Milvus] Milvus vector store is disabled via configuration");
            return;
        }
        try {
            this.milvusClient = new MilvusServiceClient(
                    ConnectParam.newBuilder()
                            .withHost(properties.getHost())
                            .withPort(properties.getPort())
                            .withConnectTimeout(properties.getConnectTimeout(), java.util.concurrent.TimeUnit.MILLISECONDS)
                            .build());
            log.info("[Milvus] Connected to Milvus {}:{}", properties.getHost(), properties.getPort());
        } catch (Exception e) {
            log.error("[Milvus] Failed to connect to Milvus: {}", e.getMessage());
            this.milvusClient = null;
        }
    }

    @Override
    public String id() {
        return "milvus";
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public boolean isAvailable() {
        return properties.isEnabled() && milvusClient != null;
    }

    @Override
    public CompletableFuture<WriteResult> writeAsync(Long workspaceId, List<VectorDocument> documents) {
        return CompletableFuture.supplyAsync(() -> {
            if (!isAvailable()) {
                return new WriteResult(0, documents.size(),
                        documents.stream().map(VectorDocument::id).toList());
            }

            String collectionName = buildCollectionName(workspaceId);
            ensureCollectionExists(collectionName);

            int successCount = 0;
            List<String> failedIds = new ArrayList<>();

            // 分批写入
            for (int i = 0; i < documents.size(); i += properties.getBatchSize()) {
                int end = Math.min(i + properties.getBatchSize(), documents.size());
                List<VectorDocument> batch = documents.subList(i, end);

                try {
                    List<InsertParam.Field> fields = new ArrayList<>();

                    List<Long> ids = new ArrayList<>();
                    List<List<Float>> vectors = new ArrayList<>();
                    List<String> contents = new ArrayList<>();
                    List<String> metadatas = new ArrayList<>();

                    for (VectorDocument doc : batch) {
                        ids.add(Long.parseLong(doc.id()));
                        List<Float> vec = new ArrayList<>(doc.embedding().length);
                        for (float v : doc.embedding()) {
                            vec.add(v);
                        }
                        vectors.add(vec);
                        contents.add(doc.content() != null ? doc.content() : "");
                        metadatas.add(doc.metadata() != null
                                ? new com.google.gson.Gson().toJson(doc.metadata()) : "{}");
                    }

                    fields.add(new InsertParam.Field("id", DataType.Int64, ids));
                    fields.add(new InsertParam.Field("embedding", DataType.FloatVector, vectors));
                    fields.add(new InsertParam.Field("content", DataType.VarChar, contents));
                    fields.add(new InsertParam.Field("metadata", DataType.VarChar, metadatas));

                    InsertParam insertParam = InsertParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFields(fields)
                            .build();

                    R<io.milvus.grpc.MutationResult> insertResp = milvusClient.insert(insertParam);
                    if (insertResp.getStatus() == 0) {
                        successCount += batch.size();
                    } else {
                        log.error("[Milvus] Insert failed: {}", insertResp.getMessage());
                        batch.forEach(doc -> failedIds.add(doc.id()));
                    }
                } catch (Exception e) {
                    log.error("[Milvus] Write batch failed: {}", e.getMessage());
                    batch.forEach(doc -> failedIds.add(doc.id()));
                }
            }

            // 写入后刷新索引
            try {
                milvusClient.flush(FlushParam.newBuilder()
                        .addCollectionName(collectionName).build());
            } catch (Exception e) {
                log.warn("[Milvus] Flush warning: {}", e.getMessage());
            }

            return new WriteResult(successCount, failedIds.size(), failedIds);
        });
    }

    @Override
    public CompletableFuture<RetrievalResult> retrieveAsync(Long workspaceId, String query, int topK) {
        return CompletableFuture.supplyAsync(() -> {
            if (!isAvailable()) {
                return new RetrievalResult(Collections.emptyList(), false, "Milvus not available");
            }

            String collectionName = buildCollectionName(workspaceId);
            ensureCollectionExists(collectionName);

            try {
                EmbeddingModel embeddingModel = getEmbeddingModel();
                if (embeddingModel == null) {
                    return new RetrievalResult(Collections.emptyList(), false, "No embedding model available");
                }

                // 向量化查询
                float[] queryEmbedding = embeddingModel.embed(query);
                List<List<Float>> searchVectors = new ArrayList<>();
                List<Float> queryVec = new ArrayList<>(queryEmbedding.length);
                for (float v : queryEmbedding) {
                    queryVec.add(v);
                }
                searchVectors.add(queryVec);

                // 执行搜索
                SearchParam searchParam = SearchParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withMetricType(MetricType.COSINE)
                        .withOutFields(List.of("id", "content", "metadata"))
                        .withTopK(topK)
                        .withVectors(searchVectors)
                        .withVectorFieldName("embedding")
                        .withParams("{\"ef\": " + properties.getEf() + "}")
                        .build();

                R<io.milvus.grpc.SearchResults> searchResp = milvusClient.search(searchParam);
                if (searchResp.getStatus() != 0) {
                    return new RetrievalResult(Collections.emptyList(), false,
                            "Milvus search failed: " + searchResp.getMessage());
                }

                SearchResultsWrapper wrapper = new SearchResultsWrapper(searchResp.getData().getResults());
                List<RetrievalHit> hits = new ArrayList<>();

                for (int i = 0; i < wrapper.getRowRecords(0).size(); i++) {
                    SearchResultsWrapper.IDScore idScore = wrapper.getIDScore(0).get(i);
                    float score = idScore.getScore();

                    if (score < 0.7f) {
                        continue; // 相似度阈值
                    }

                    String content = String.valueOf(
                            wrapper.getFieldData("content", 0).get(i));
                    String metadataJson = String.valueOf(
                            wrapper.getFieldData("metadata", 0).get(i));

                    Map<String, Object> metadata = parseMetadata(metadataJson);

                    hits.add(new RetrievalHit(
                            String.valueOf(idScore.getLongID()),
                            content, score, metadata));
                }

                return new RetrievalResult(hits, false, null);

            } catch (Exception e) {
                log.error("[Milvus] Retrieve failed for workspaceId={}: {}", workspaceId, e.getMessage());
                return new RetrievalResult(Collections.emptyList(), false, e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Long workspaceId, List<String> documentIds) {
        return CompletableFuture.runAsync(() -> {
            if (!isAvailable()) return;
            String collectionName = buildCollectionName(workspaceId);
            List<Long> ids = documentIds.stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
            try {
                milvusClient.delete(DeleteParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withExpr("id in " + ids).build());
            } catch (Exception e) {
                log.error("[Milvus] Delete failed: {}", e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<Void> clearAsync(Long workspaceId) {
        return CompletableFuture.runAsync(() -> {
            if (!isAvailable()) return;
            String collectionName = buildCollectionName(workspaceId);
            try {
                milvusClient.dropCollection(DropCollectionParam.newBuilder()
                        .withCollectionName(collectionName).build());
                initializedCollections.remove(collectionName);
                log.info("[Milvus] Dropped collection for workspaceId={}", workspaceId);
            } catch (Exception e) {
                log.error("[Milvus] Clear failed: {}", e.getMessage());
            }
        });
    }

    @Override
    public CompletableFuture<VectorStats> getStatsAsync(Long workspaceId) {
        return CompletableFuture.supplyAsync(() -> {
            if (!isAvailable()) return new VectorStats(0, properties.getDimension(), "N/A");
            String collectionName = buildCollectionName(workspaceId);
            try {
                R<io.milvus.grpc.GetCollectionStatisticsResponse> resp = milvusClient.getCollectionStatistics(
                        GetCollectionStatisticsParam.newBuilder()
                                .withCollectionName(collectionName).build());
                if (resp.getStatus() == 0) {
                    long rowCount = resp.getData().getStatsList().stream()
                            .filter(s -> "row_count".equals(s.getKey()))
                            .mapToLong(s -> Long.parseLong(s.getValue()))
                            .findFirst().orElse(0);
                    return new VectorStats(rowCount, properties.getDimension(), properties.getIndexType());
                }
            } catch (Exception e) {
                log.warn("[Milvus] Get stats failed: {}", e.getMessage());
            }
            return new VectorStats(0, properties.getDimension(), properties.getIndexType());
        });
    }

    // ---- 内部方法 ----

    private String buildCollectionName(Long workspaceId) {
        return properties.getCollectionPrefix() + "_" + workspaceId + "_vectors";
    }

    private void ensureCollectionExists(String collectionName) {
        if (initializedCollections.contains(collectionName)) {
            return;
        }

        try {
            R<Boolean> hasColl = milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(collectionName).build());

            if (hasColl.getStatus() == 0 && Boolean.TRUE.equals(hasColl.getData())) {
                initializedCollections.add(collectionName);
                return;
            }

            // 创建 Collection
            CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .addFieldType(io.milvus.grpc.FieldType.newBuilder()
                            .withName("id").withDataType(DataType.Int64)
                            .withIsPrimaryKey(true).withAutoID(false).build())
                    .addFieldType(io.milvus.grpc.FieldType.newBuilder()
                            .withName("embedding").withDataType(DataType.FloatVector)
                            .withDimension(properties.getDimension()).build())
                    .addFieldType(io.milvus.grpc.FieldType.newBuilder()
                            .withName("content").withDataType(DataType.VarChar)
                            .withMaxLength(65535).build())
                    .addFieldType(io.milvus.grpc.FieldType.newBuilder()
                            .withName("metadata").withDataType(DataType.VarChar)
                            .withMaxLength(10240).build())
                    .build();

            R<io.milvus.grpc.Status> createResp = milvusClient.createCollection(createParam);
            if (createResp.getStatus() != 0) {
                log.error("[Milvus] Failed to create collection {}: {}", collectionName, createResp.getMessage());
                return;
            }

            // 创建索引
            milvusClient.createIndex(CreateIndexParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFieldName("embedding")
                    .withIndexType(io.milvus.grpc.IndexType.valueOf(properties.getIndexType()))
                    .withMetricType(MetricType.COSINE)
                    .withExtraParam(String.format("{\"M\": %d, \"efConstruction\": %d}",
                            properties.getHnswM(), properties.getEfConstruction()))
                    .build());

            // 加载 Collection 到内存
            milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName).build());

            initializedCollections.add(collectionName);
            log.info("[Milvus] Created collection: {}", collectionName);

        } catch (Exception e) {
            log.error("[Milvus] Failed to ensure collection {}: {}", collectionName, e.getMessage());
        }
    }

    /**
     * 获取默认 Embedding 模型。
     * 使用 ModelConfigService 提供的 {@code findFirstEnabledEmbedding()} 方法。
     */
    private EmbeddingModel getEmbeddingModel() {
        try {
            ModelConfigEntity config = modelConfigService.findFirstEnabledEmbedding();
            if (config == null) {
                log.warn("[Milvus] No enabled embedding model config found");
                return null;
            }
            return embeddingModelFactory.build(config);
        } catch (Exception e) {
            log.error("[Milvus] Failed to build embedding model: {}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMetadata(String json) {
        if (json == null || json.isEmpty() || "{}".equals(json)) {
            return Map.of();
        }
        try {
            return new com.google.gson.Gson().fromJson(json, Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }
}
