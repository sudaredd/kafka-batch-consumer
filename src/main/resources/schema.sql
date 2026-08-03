CREATE TABLE IF NOT EXISTS job_status (
    job_id      VARCHAR(255) PRIMARY KEY,
    status      VARCHAR(50)  NOT NULL,
    updated_at  TIMESTAMP    NOT NULL
);
