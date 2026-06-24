-- V100: Embedding retry task table.
-- Records failed chunk-embedding operations from the async embedding pipeline
-- (MilvusAsyncEmbeddingService). The EmbeddingRetryService cron job picks up
-- PENDING rows whose next_retry_time has elapsed and re-attempts the embedding
-- + vector-store write. After MAX_RETRY_COUNT (5) failures the row is marked
-- ABORTED.

CREATE TABLE IF NOT EXISTS mate_embedding_task (
    id               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    chunk_id         BIGINT       NOT NULL,
    kb_id            BIGINT,
    workspace_id     BIGINT,
    status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    retry_count      INT          NOT NULL DEFAULT 0,
    next_retry_time  DATETIME(3),
    error_message    MEDIUMTEXT,
    created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    INDEX idx_et_status_time (status, next_retry_time),
    INDEX idx_et_chunk (chunk_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Embedding retry task queue for async vectorization pipeline.';
