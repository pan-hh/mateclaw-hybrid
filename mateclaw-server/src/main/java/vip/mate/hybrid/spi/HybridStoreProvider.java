package vip.mate.hybrid.spi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 向量存储提供者 SPI 接口。
 * <p>
 * 抽象向量存储后端（Milvus、Elasticsearch、MySQL 等）的统一操作契约，
 * 通过 Java {@link java.util.ServiceLoader} 机制实现可插拔扩展。
 * <p>
 * 实现类应通过 {@code @Component} 注册到 Spring 容器并同时声明在
 * {@code META-INF/services/vip.mate.hybrid.spi.HybridStoreProvider} 中。
 *
 * @author MateClaw Team
 */
public interface HybridStoreProvider {

    /**
     * 提供者唯一标识，如 "milvus"、"elasticsearch"、"fallback"。
     */
    String id();

    /**
     * 提供者优先级，数字越小优先级越高。
     * 默认 100。HybridStoreManager 按此排序选择 active provider。
     */
    default int order() {
        return 100;
    }

    /**
     * 当前提供者是否可用（已启用且连接正常）。
     */
    boolean isAvailable();

    /**
     * 异步写入文档到向量存储。
     *
     * @param workspaceId 工作空间 ID（多租户隔离）
     * @param documents   待写入的文档列表
     * @return 写入结果
     */
    CompletableFuture<WriteResult> writeAsync(Long workspaceId, List<VectorDocument> documents);

    /**
     * 异步检索与查询语义相关的文档。
     *
     * @param workspaceId 工作空间 ID
     * @param query       查询文本
     * @param topK        返回结果数
     * @return 检索结果
     */
    CompletableFuture<RetrievalResult> retrieveAsync(Long workspaceId, String query, int topK);

    /**
     * 异步删除指定文档。
     *
     * @param workspaceId  工作空间 ID
     * @param documentIds  文档 ID 列表
     */
    CompletableFuture<Void> deleteAsync(Long workspaceId, List<String> documentIds);

    /**
     * 异步清空整个工作空间的向量数据。
     *
     * @param workspaceId 工作空间 ID
     */
    CompletableFuture<Void> clearAsync(Long workspaceId);

    /**
     * 异步获取存储统计信息。
     *
     * @param workspaceId 工作空间 ID
     * @return 统计信息
     */
    CompletableFuture<VectorStats> getStatsAsync(Long workspaceId);

    // ---- 数据记录类型 ----

    /**
     * 向量文档。
     */
    record VectorDocument(String id, String content, float[] embedding,
                          Map<String, Object> metadata) {
    }

    /**
     * 单条检索命中结果。
     */
    record RetrievalHit(String id, String content, float score,
                        Map<String, Object> metadata) {
    }

    /**
     * 写入操作结果。
     */
    record WriteResult(int successCount, int failedCount, List<String> failedIds) {
    }

    /**
     * 检索操作结果。
     *
     * @param hits          命中结果列表
     * @param fallbackUsed  是否使用了降级
     * @param errorMessage  错误信息（成功时为 null）
     */
    record RetrievalResult(List<RetrievalHit> hits, boolean fallbackUsed, String errorMessage) {
        public boolean success() {
            return errorMessage == null && !hits.isEmpty();
        }
    }

    /**
     * 向量存储统计信息。
     */
    record VectorStats(long totalVectors, int dimension, String indexType) {
    }
}
