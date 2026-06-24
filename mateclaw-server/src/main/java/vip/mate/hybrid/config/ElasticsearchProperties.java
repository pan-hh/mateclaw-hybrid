package vip.mate.hybrid.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Elasticsearch 全文检索配置。
 * <p>
 * 前缀：{@code mate.hybrid.elasticsearch}
 *
 * @author MateClaw Team
 */
@Data
@ConfigurationProperties(prefix = "mate.hybrid.elasticsearch")
public class ElasticsearchProperties {

    /** 是否启用 Elasticsearch */
    private boolean enabled = true;

    /** ES 服务地址 */
    private String host = "localhost";

    /** ES HTTP 端口 */
    private int port = 9200;

    /** 协议方案（http / https） */
    private String scheme = "http";

    /** 用户名（可选，ES 8.x 默认需要） */
    private String username;

    /** 密码（可选） */
    private String password;

    /** 索引名称前缀，实际名称 = {prefix}_{workspaceId} */
    private String indexPrefix = "wiki";

    /** 连接超时（毫秒） */
    private int connectTimeout = 5000;

    /** Socket 读取超时（毫秒） */
    private int socketTimeout = 30000;

    /** 最大重试超时（毫秒） */
    private int maxRetryTimeout = 60000;

    /** 批量写入大小 */
    private int batchSize = 100;
}
