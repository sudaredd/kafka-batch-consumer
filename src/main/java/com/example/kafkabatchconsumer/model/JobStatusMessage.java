package com.example.kafkabatchconsumer.model;

import java.time.LocalDateTime;

/**
 * Represents a job-status event consumed from Kafka.
 */
public record JobStatusMessage(
        String jobId,
        String status,
        LocalDateTime updatedAt
) {}
