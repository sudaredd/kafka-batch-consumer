package com.example.kafkabatchconsumer;

import com.example.kafkabatchconsumer.model.JobStatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class JobStatusBatchConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        jdbcTemplate.execute("DELETE FROM job_status");
    }

    @Test
    void testValidBatchPublishing() throws Exception {
        JobStatusMessage msg1 = new JobStatusMessage("JOB-101", "COMPLETED", LocalDateTime.now());
        JobStatusMessage msg2 = new JobStatusMessage("JOB-102", "IN_PROGRESS", LocalDateTime.now());

        kafkaTemplate.send("job-status", msg1.jobId(), objectMapper.writeValueAsBytes(msg1)).get();
        kafkaTemplate.send("job-status", msg2.jobId(), objectMapper.writeValueAsBytes(msg2)).get();

        // Poll until consumer processes the batch (up to 10s)
        int count = 0;
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM job_status", Integer.class);
            if (count >= 2) {
                break;
            }
            Thread.sleep(500);
        }

        assertThat(count).isGreaterThanOrEqualTo(2);
    }

    @Test
    void testCorruptedMessageTriggersFallbackAndDLT() throws Exception {
        JobStatusMessage validMsg = new JobStatusMessage("JOB-201", "SUCCESS", LocalDateTime.now());
        byte[] invalidMsgBytes = "INVALID_JSON_PAYLOAD".getBytes();

        kafkaTemplate.send("job-status", validMsg.jobId(), objectMapper.writeValueAsBytes(validMsg)).get();
        kafkaTemplate.send("job-status", "JOB-BAD", invalidMsgBytes).get();

        // Allow consumer processing, retry, and DLT forwarding
        Thread.sleep(6000);

        // Verify valid message persisted to DB
        Integer validCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM job_status WHERE job_id = 'JOB-201'", Integer.class);
        assertThat(validCount).isEqualTo(1);

        // Verify DLT received the bad message
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-verifier-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        DefaultKafkaConsumerFactory<String, byte[]> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        boolean foundBadRecord = false;
        long deadline = System.currentTimeMillis() + 15000;
        try (Consumer<String, byte[]> dltConsumer = cf.createConsumer()) {
            org.apache.kafka.common.TopicPartition dltTp = new org.apache.kafka.common.TopicPartition("job-status.DLT", 0);
            dltConsumer.assign(Collections.singletonList(dltTp));
            dltConsumer.seekToBeginning(Collections.singletonList(dltTp));

            while (System.currentTimeMillis() < deadline && !foundBadRecord) {
                ConsumerRecords<String, byte[]> records = dltConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if ("JOB-BAD".equals(record.key())) {
                        foundBadRecord = true;
                        break;
                    }
                }
            }
            assertThat(foundBadRecord).isTrue();
        }
    }
}
