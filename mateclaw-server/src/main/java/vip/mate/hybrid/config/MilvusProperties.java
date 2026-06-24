package vip.mate.hybrid.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Milvus 向量数据库配置。
 * <p>
 * 前缀：{@code mate.hybrid.milvus}
 *
 * @author MateClaw Team
 */
@Data
@ConfigurationProperties(prefix = "mate.hybrid.milvus")
public class MilvusProperties {

    /** 是否启用 Milvus */
    private boolean enabled = true;

    /** Milvus 服务地址 */
    private String host = "localhost";

    /** Milvus gRPC 端口 */
    private int port = 19530;

    /** 数据库名称 */
    private String database = "default";

    /** Collection 名称前缀，实际名称 = {prefix}_{workspaceId}_vectors */
    private String collectionPrefix = "workspace";

    /** 向量维度（与 Embedding 模型对齐，DashScope text-embedding-v3 = 1024） */
    private int dimension = 1024;

    /** 索引类型：HNSW / IVF_FLAT / IVF_SQ8 */
    private String indexType = "HNSW";

    /** HNSW 图的 M 参数（每个节点的最大连接数） */
    private int hnswM = 16;

    /** HNSW 构建时的搜索宽度 */
    private int efConstruction = 200;

    /** 查询时的搜索宽度 */
    private int ef = 100;

    /** gRPC 连接超时（毫秒） */
    private int connectTimeout = 5000;

    /** 查询超时（毫秒） */
    private int queryTimeout = 30000;

    /** 批量写入大小 */
    private int batchSize = 100;

    /** 最大重试次数 */
    private int maxRetries = 3;
}
