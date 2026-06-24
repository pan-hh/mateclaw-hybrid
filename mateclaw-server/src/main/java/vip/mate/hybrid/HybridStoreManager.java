package vip.mate.hybrid;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import vip.mate.hybrid.spi.HybridStoreProvider;

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 向量存储管理器。
 * <p>
 * 核心职责：
 * <ul>
 *   <li>通过 Java SPI ({@link ServiceLoader}) 加载所有 {@link HybridStoreProvider} 实现</li>
 *   <li>按 {@link HybridStoreProvider#order()} 排序，选择优先级最高且可用的作为 active provider</li>
 *   <li>提供 ES 全文检索的单独访问入口</li>
 *   <li>检索失败时自动降级到 fallback 提供者</li>
 * </ul>
 * <p>
 * 注意：HybridStoreProvider 同时作为 Spring {@code @Component} 注册。
 * SPI 加载和 Spring DI 两套机制共存，互不冲突。
 *
 * @author MateClaw Team
 */
@Slf4j
@Component
public class HybridStoreManager {

    /** SPI 加载到的所有 Provider（线程安全列表） */
    private final List<HybridStoreProvider> providers = new CopyOnWriteArrayList<>();

    /** 当前活跃的向量检索提供者（Milvus / Fallback） */
    private volatile HybridStoreProvider activeProvider;

    /** ES 全文检索提供者（单独持有以支持双路并行检索） */
    private volatile HybridStoreProvider esProvider;

    @PostConstruct
    void init() {
        ServiceLoader<HybridStoreProvider> loader = ServiceLoader.load(HybridStoreProvider.class);

        int loaded = 0;
        for (HybridStoreProvider provider : loader) {
            providers.add(provider);
            loaded++;
            log.info("[HybridStore] Loaded provider: id={}, order={}, available={}",
                    provider.id(), provider.order(), provider.isAvailable());

            if ("elasticsearch".equals(provider.id())) {
                esProvider = provider;
            }
        }

        if (loaded == 0) {
            log.warn("[HybridStore] No HybridStoreProvider implementations found via SPI. "
                    + "Ensure META-INF/services/vip.mate.hybrid.spi.HybridStoreProvider exists "
                    + "and provider classes are on the classpath.");
        }

        // 按 order 排序
        providers.sort(Comparator.comparingInt(HybridStoreProvider::order));
        refreshActiveProvider();
    }

    /**
     * 刷新 active provider：选择第一个可用且非 ES 的提供者。
     * <p>
     * ES provider 通过 {@link #getEsProvider()} 单独访问，不作为主向量检索入口。
     */
    public synchronized void refreshActiveProvider() {
        for (HybridStoreProvider provider : providers) {
            if (provider.isAvailable() && !"elasticsearch".equals(provider.id())) {
                activeProvider = provider;
                log.info("[HybridStore] Active provider set to: {}", provider.id());
                return;
            }
        }
        log.warn("[HybridStore] No available (non-ES) provider found; vector retrieval will fail");
        activeProvider = null;
    }

    /**
     * 获取当前的活跃向量检索提供者。
     *
     * @return 活跃提供者，若无可用则返回 null
     */
    public HybridStoreProvider getActiveProvider() {
        if (activeProvider == null || !activeProvider.isAvailable()) {
            refreshActiveProvider();
        }
        return activeProvider;
    }

    /**
     * 获取 Elasticsearch 全文检索提供者（可能为 null，表示 ES 不可用）。
     */
    public HybridStoreProvider getEsProvider() {
        if (esProvider != null && !esProvider.isAvailable()) {
            return null;
        }
        return esProvider;
    }

    /**
     * 按 ID 查找指定提供者。
     *
     * @param providerId 提供者 ID（如 "milvus"、"elasticsearch"、"fallback"）
     * @return 找到的提供者，未找到返回 null
     */
    public HybridStoreProvider getProvider(String providerId) {
        return providers.stream()
                .filter(p -> p.id().equals(providerId))
                .findFirst()
                .orElse(null);
    }

    /**
     * 执行向量检索（同步阻塞）。
     * <p>
     * 优先使用 active provider，失败时自动降级到 fallback。
     *
     * @param workspaceId 工作空间 ID
     * @param query       查询文本
     * @param topK        返回结果数
     * @return 检索结果
     */
    public HybridStoreProvider.RetrievalResult retrieve(Long workspaceId, String query, int topK) {
        HybridStoreProvider provider = getActiveProvider();
        if (provider == null) {
            log.error("[HybridStore] No provider available for retrieval");
            return new HybridStoreProvider.RetrievalResult(List.of(), false, "No provider available");
        }

        try {
            return provider.retrieveAsync(workspaceId, query, topK).get();
        } catch (Exception e) {
            log.error("[HybridStore] Retrieval failed via {}: {}", provider.id(), e.getMessage());

            // 降级到 fallback
            HybridStoreProvider fallback = getProvider("fallback");
            if (fallback != null && !"fallback".equals(provider.id())) {
                try {
                    log.info("[HybridStore] Attempting fallback retrieval via {}", fallback.id());
                    HybridStoreProvider.RetrievalResult fallbackResult =
                            fallback.retrieveAsync(workspaceId, query, topK).get();
                    return new HybridStoreProvider.RetrievalResult(
                            fallbackResult.hits(), true, fallbackResult.errorMessage());
                } catch (Exception fallbackEx) {
                    log.error("[HybridStore] Fallback retrieval also failed: {}", fallbackEx.getMessage());
                }
            }

            return new HybridStoreProvider.RetrievalResult(List.of(), true, e.getMessage());
        }
    }

    /**
     * 返回所有已加载的 Provider（只读快照）。
     */
    public List<HybridStoreProvider> getAllProviders() {
        return List.copyOf(providers);
    }
}
