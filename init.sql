CREATE TABLE IF NOT EXISTS job_status (
    job_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(255),
    updated_at TIMESTAMP
);
