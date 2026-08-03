package com.example.kafkabatchconsumer.consumer;

import com.example.kafkabatchconsumer.model.JobStatusMessage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

@Component
public class JobStatusBatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(JobStatusBatchConsumer.class);

    private static final String UPSERT_SQL = """
            INSERT INTO job_status (job_id, status, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (job_id) DO UPDATE
                SET status     = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
            """;

    private final JdbcTemplate jdbcTemplate;

    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    public JobStatusBatchConsumer(JdbcTemplate jdbcTemplate, com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "job-status", groupId = "job-status-group")
    public void onBatch(List<ConsumerRecord<String, byte[]>> records) {
        log.info("Received batch of {} raw byte records", records.size());

        // ── 1. Parse all records first ──
        // We parse them into an array/list so we can run the batch update.
        // If parsing fails, we fall back to single-record processing so the exact failing offset is identified.
        JobStatusMessage[] messages = new JobStatusMessage[records.size()];
        boolean parseSuccess = true;

        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            try {
                byte[] val = record.value();
                if (val != null) {
                    messages[i] = objectMapper.readValue(val, JobStatusMessage.class);
                    log.info("Parsed record [index={}, offset={}, key={}]: jobId={}, status={}",
                            i, record.offset(), record.key(), messages[i].jobId(), messages[i].status());
                }
            } catch (Exception parseEx) {
                log.warn("Failed to parse record [index={}, offset={}, key={}] during pre-parse step",
                        i, record.offset(), record.key());
                parseSuccess = false;
                break;
            }
        }

        if (parseSuccess) {
            try {
                // ── 2. Batch-first: attempt a single batchUpdate ──
                jdbcTemplate.batchUpdate(UPSERT_SQL, records, records.size(),
                        (ps, record) -> {
                            int idx = records.indexOf(record);
                            JobStatusMessage msg = messages[idx];
                            if (msg != null) {
                                ps.setString(1, msg.jobId());
                                ps.setString(2, msg.status());
                                ps.setTimestamp(3, Timestamp.valueOf(msg.updatedAt()));
                            } else {
                                ps.setNull(1, java.sql.Types.VARCHAR);
                                ps.setNull(2, java.sql.Types.VARCHAR);
                                ps.setNull(3, java.sql.Types.TIMESTAMP);
                            }
                        });

                log.info("Batch upsert succeeded for {} records", records.size());
                return; // successfully processed the whole batch

            } catch (Exception batchEx) {
                log.warn("Batch upsert failed, falling back to single-record processing", batchEx);
            }
        }

        // ── 3. Fallback: process and insert one-by-one ────────────────
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, byte[]> record = records.get(i);
            try {
                byte[] val = record.value();
                if (val == null) {
                    throw new IllegalArgumentException("Record value is null");
                }
                JobStatusMessage msg = objectMapper.readValue(val, JobStatusMessage.class);
                
                jdbcTemplate.update(UPSERT_SQL,
                        msg.jobId(),
                        msg.status(),
                        Timestamp.valueOf(msg.updatedAt()));

                log.info("Successfully upserted single record [index={}, offset={}, key={}]: jobId={}, status={}",
                        i, record.offset(), record.key(), msg.jobId(), msg.status());

            } catch (Exception singleEx) {
                log.error("Single-record processing/insert failed for record at index {} "
                          + "(offset={}, key={}): {}",
                        i, record.offset(), record.key(), singleEx.getMessage());

                // Throw BatchListenerFailedException referencing
                // the exact failing record so the DefaultErrorHandler
                // can route it to the Dead Letter Topic.
                throw new BatchListenerFailedException(
                        "Failed to process record: " + record.key(), singleEx, record);
            }
        }

        log.info("Single-record fallback completed successfully for {} records", records.size());
    }
}
