package vip.mate.hybrid.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 向量存储异步线程池配置。
 * <p>
 * 用于 {@code MilvusAsyncEmbeddingService} 和 {@code ElasticsearchSyncService}
 * 的后台 Embedding / 同步任务线程池。
 * <p>
 * 前缀：{@code mate.hybrid.async}
 *
 * @author MateClaw Team
 */
@Data
@ConfigurationProperties(prefix = "mate.hybrid.async")
public class AsyncPoolProperties {

    /** 核心线程数（默认 4） */
    private int corePoolSize = 4;

    /** 最大线程数（默认 8） */
    private int maxPoolSize = 8;

    /** 任务队列容量（默认 1000） */
    private int queueCapacity = 1000;

    /** 非核心线程空闲存活时间（秒） */
    private int keepAliveSeconds = 60;

    /** 线程名前缀 */
    private String threadNamePrefix = "embedding-";

    /** 优雅关闭最大等待时间（秒） */
    private int awaitTerminationSeconds = 60;
}
