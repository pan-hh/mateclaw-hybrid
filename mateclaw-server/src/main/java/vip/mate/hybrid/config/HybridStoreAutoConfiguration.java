package vip.mate.hybrid.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import vip.mate.hybrid.HybridStoreManager;
import vip.mate.hybrid.provider.ElasticsearchProvider;
import vip.mate.hybrid.provider.FallbackHybridStoreProvider;
import vip.mate.hybrid.provider.MilvusHybridStoreProvider;
import vip.mate.hybrid.service.DataConsistencyService;
import vip.mate.hybrid.service.DocumentChunkService;
import vip.mate.hybrid.service.ElasticsearchSyncService;
import vip.mate.hybrid.service.EmbeddingRetryService;
import vip.mate.hybrid.service.MilvusAsyncEmbeddingService;
import vip.mate.hybrid.tool.KnowledgeRetrievalTool;
import vip.mate.wiki.service.HybridRetriever;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 向量存储模块自动配置。
 * <p>
 * 启用配置属性绑定（Milvus / Elasticsearch / AsyncPool），
 * 注册所有向量存储相关 Bean（Provider、Service、Manager、Tool），
 * 创建 Embedding 异步处理线程池。
 *
 * @author MateClaw Team
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({
        MilvusProperties.class,
        ElasticsearchProperties.class,
        AsyncPoolProperties.class
})
@Import({
        // Providers
        MilvusHybridStoreProvider.class,
        ElasticsearchProvider.class,
        FallbackHybridStoreProvider.class,

        // Manager
        HybridStoreManager.class,

        // Services
        DocumentChunkService.class,
        MilvusAsyncEmbeddingService.class,
        ElasticsearchSyncService.class,
        EmbeddingRetryService.class,
        DataConsistencyService.class,

        // Retrieval & Tool
        HybridRetriever.class,
        KnowledgeRetrievalTool.class
})
public class HybridStoreAutoConfiguration {

    /**
     * 创建 Embedding 异步处理线程池。
     * <p>
     * 用于 {@link MilvusAsyncEmbeddingService} 和 {@link ElasticsearchSyncService}
     * 的后台 Embedding / 同步任务。使用 CallerRunsPolicy 拒绝策略，
     * 队列满时由调用线程执行，保证不丢任务。
     */
    @Bean(name = "embeddingThreadPool")
    @ConditionalOnMissingBean(name = "embeddingThreadPool")
    public ThreadPoolTaskExecutor embeddingThreadPool(AsyncPoolProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getCorePoolSize());
        executor.setMaxPoolSize(properties.getMaxPoolSize());
        executor.setQueueCapacity(properties.getQueueCapacity());
        executor.setKeepAliveSeconds(properties.getKeepAliveSeconds());
        executor.setThreadNamePrefix(properties.getThreadNamePrefix());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(properties.getAwaitTerminationSeconds());
        executor.initialize();

        log.info("[HybridStore] Embedding thread pool initialized: core={}, max={}, queue={}",
                properties.getCorePoolSize(), properties.getMaxPoolSize(), properties.getQueueCapacity());
        return executor;
    }
}
