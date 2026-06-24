package vip.mate.hybrid.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vip.mate.llm.embedding.EmbeddingModelFactory;
import vip.mate.llm.model.ModelConfigEntity;
import vip.mate.llm.service.ModelConfigService;
import vip.mate.hybrid.HybridStoreManager;
import vip.mate.hybrid.spi.HybridStoreProvider;
import vip.mate.wiki.model.EmbeddingTaskEntity;
import vip.mate.wiki.model.WikiChunkEntity;
import vip.mate.wiki.repository.EmbeddingTaskMapper;
import vip.mate.wiki.repository.WikiChunkMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 嵌入重试服务。
 * <p>
 * 每分钟扫描 {@code mate_embedding_task} 表，重新处理失败的 Embedding 任务。
 * 支持指数退避重试策略，最多 5 次，失败后标记为 ABORTED。
 *
 * @author MateClaw Team
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EmbeddingRetryService {

    private final EmbeddingTaskMapper embeddingTaskMapper;
    private final WikiChunkMapper wikiChunkMapper;
    private final HybridStoreManager HybridStoreManager;
    private final EmbeddingModelFactory embeddingModelFactory;
    private final ModelConfigService modelConfigService;

    private static final int MAX_RETRY_COUNT = 5;
    private static final int BATCH_SIZE = 100;

    /**
     * 记录一个失败的 chunk（供异步流水线调用）。
     */
    public void recordFailedChunk(Long chunkId, Long kbId, Long workspaceId) {
        try {
            EmbeddingTaskEntity task = new EmbeddingTaskEntity();
            task.setChunkId(chunkId);
            task.setKbId(kbId);
            task.setWorkspaceId(workspaceId);
            task.setStatus("PENDING");
            task.setRetryCount(0);
            task.setNextRetryTime(LocalDateTime.now().plusMinutes(5));
            task.setErrorMessage("Embedding failed");

            embeddingTaskMapper.insert(task);
            log.info("[RetryService] Recorded failed chunk {} for kbId={}", chunkId, kbId);
        } catch (Exception e) {
            log.error("[RetryService] Failed to record failed chunk {}: {}", chunkId, e.getMessage());
        }
    }

    /**
     * 批量记录失败的文档。
     */
    public void recordFailedDocuments(Long kbId, Long workspaceId, List<String> failedIds) {
        if (failedIds == null || failedIds.isEmpty()) return;
        for (String id : failedIds) {
            try {
                recordFailedChunk(Long.parseLong(id), kbId, workspaceId);
            } catch (NumberFormatException e) {
                log.warn("[RetryService] Invalid chunk id: {}", id);
            }
        }
    }

    /**
     * 定时扫描重试队列（每 60 秒一次）。
     */
    @Scheduled(fixedDelay = 60_000)
    @Transactional(rollbackFor = Exception.class)
    public void processRetryQueue() {
        List<EmbeddingTaskEntity> tasks = embeddingTaskMapper.selectPendingTasks(BATCH_SIZE);
        if (tasks.isEmpty()) return;

        log.info("[RetryService] Processing {} pending tasks", tasks.size());

        ModelConfigEntity modelConfig = modelConfigService.findFirstEnabledEmbedding();
        if (modelConfig == null) {
            log.error("[RetryService] No embedding model available for retry");
            return;
        }

        var embeddingModel = embeddingModelFactory.build(modelConfig);

        for (EmbeddingTaskEntity task : tasks) {
            try {
                processTask(task, embeddingModel);
            } catch (Exception e) {
                log.error("[RetryService] Failed to process task {}: {}", task.getId(), e.getMessage());
            }
        }
    }

    private void processTask(EmbeddingTaskEntity task, org.springframework.ai.embedding.EmbeddingModel embeddingModel) {
        WikiChunkEntity chunk = wikiChunkMapper.selectById(task.getChunkId());
        if (chunk == null) {
            log.warn("[RetryService] Chunk {} not found, removing task", task.getChunkId());
            embeddingTaskMapper.deleteById(task.getId());
            return;
        }

        try {
            float[] embedding = embeddingModel.embed(chunk.getContent());

            var document = new HybridStoreProvider.VectorDocument(
                    chunk.getId().toString(),
                    chunk.getContent(),
                    embedding,
                    Map.of("chunkId", chunk.getId(), "kbId", task.getKbId(),
                            "rawId", chunk.getRawId() != null ? chunk.getRawId() : ""));

            HybridStoreProvider provider = HybridStoreManager.getActiveProvider();
            if (provider != null) {
                var result = provider.writeAsync(task.getWorkspaceId(), List.of(document)).get();
                if (result.successCount() > 0) {
                    task.setStatus("SUCCESS");
                    task.setErrorMessage(null);
                    log.info("[RetryService] Task {} succeeded after {} retries",
                            task.getId(), task.getRetryCount());
                } else {
                    updateTaskForRetry(task, "Write failed");
                }
            } else {
                updateTaskForRetry(task, "No active provider");
            }

        } catch (Exception e) {
            updateTaskForRetry(task, e.getMessage());
        }

        embeddingTaskMapper.updateById(task);
    }

    private void updateTaskForRetry(EmbeddingTaskEntity task, String errorMessage) {
        int newRetryCount = (task.getRetryCount() != null ? task.getRetryCount() : 0) + 1;

        if (newRetryCount >= MAX_RETRY_COUNT) {
            task.setStatus("ABORTED");
            task.setErrorMessage("Max retry count exceeded: " + errorMessage);
            log.error("[RetryService] Task {} aborted after {} retries", task.getId(), newRetryCount);
        } else {
            long delayMinutes = (long) Math.pow(2, newRetryCount) * 5; // 指数退避: 5,10,20,40 分钟
            task.setStatus("PENDING");
            task.setRetryCount(newRetryCount);
            task.setNextRetryTime(LocalDateTime.now().plusMinutes(delayMinutes));
            task.setErrorMessage(errorMessage);
            log.warn("[RetryService] Task {} scheduled for retry {} in {} minutes",
                    task.getId(), newRetryCount, delayMinutes);
        }
    }

    public int getPendingTaskCount() {
        return embeddingTaskMapper.countPendingTasks();
    }

    public List<EmbeddingTaskEntity> getPendingTasks() {
        return embeddingTaskMapper.selectPendingTasks(Integer.MAX_VALUE);
    }

    /**
     * 重试所有失败任务（手动触发）。
     */
    @Transactional(rollbackFor = Exception.class)
    public void retryAllFailedTasks() {
        List<EmbeddingTaskEntity> tasks = embeddingTaskMapper.selectByStatus("FAILED");
        log.info("[RetryService] Resetting {} failed tasks for immediate retry", tasks.size());
        for (EmbeddingTaskEntity task : tasks) {
            task.setStatus("PENDING");
            task.setNextRetryTime(LocalDateTime.now());
            embeddingTaskMapper.updateById(task);
        }
    }
}
