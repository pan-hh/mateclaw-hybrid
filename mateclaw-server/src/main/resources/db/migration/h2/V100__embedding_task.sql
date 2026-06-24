-- V100: Embedding retry task table.
-- Records failed chunk-embedding operations from the async embedding pipeline
-- (MilvusAsyncEmbeddingService). The EmbeddingRetryService cron job picks up
-- PENDING rows whose next_retry_time has elapsed and re-attempts the embedding
-- + vector-store write. After MAX_RETRY_COUNT (5) failures the row is marked
-- ABORTED. H2 dialect uses CLOB for TEXT and TIMESTAMP for datetime columns.

CREATE TABLE IF NOT EXISTS mate_embedding_task (
    id               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    chunk_id         BIGINT       NOT NULL,
    kb_id            BIGINT,
    workspace_id     BIGINT,
    status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    retry_count      INT          NOT NULL DEFAULT 0,
    next_retry_time  TIMESTAMP,
    error_message    CLOB,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_embedding_task_status_time
    ON mate_embedding_task (status, next_retry_time);

CREATE INDEX IF NOT EXISTS idx_embedding_task_chunk
    ON mate_embedding_task (chunk_id);
