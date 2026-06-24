package vip.mate.wiki.model;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 嵌入重试任务实体。
 * <p>
 * 记录异步 Embedding 流水线中失败的 chunk，由 {@code EmbeddingRetryService}
 * 定时扫描重试。最多重试 5 次，失败后标记为 ABORTED。
 *
 * @author MateClaw Team
 */
@Data
@TableName("mate_embedding_task")
public class EmbeddingTaskEntity {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 关联的 chunk ID */
    @TableField("chunk_id")
    private Long chunkId;

    /** 知识库 ID */
    @TableField("kb_id")
    private Long kbId;

    /** 工作空间 ID */
    @TableField("workspace_id")
    private Long workspaceId;

    /** 任务状态：PENDING / SUCCESS / FAILED / ABORTED */
    @TableField("status")
    private String status;

    /** 已重试次数 */
    @TableField("retry_count")
    private Integer retryCount;

    /** 下次重试时间 */
    @TableField("next_retry_time")
    private LocalDateTime nextRetryTime;

    /** 错误信息 */
    @TableField("error_message")
    private String errorMessage;

    @TableField(value = "created_at", fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(value = "updated_at", fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
