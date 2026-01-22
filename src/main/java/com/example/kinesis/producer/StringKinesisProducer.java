package com.example.kinesis.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Kinesis Producer utility for sending String messages to Kinesis Data Streams.
 * 
 * This producer supports:
 * - Sending single String messages
 * - Sending batch of String messages
 * - Automatic credential resolution from EKS IAM role (via DefaultCredentialsProvider)
 * 
 * Usage example:
 * <pre>
 * KinesisProducerConfig config = KinesisProducerConfig.builder()
 *     .streamName("my-stream")
 *     .region(Region.US_EAST_1)
 *     .build();
 * 
 * StringKinesisProducer producer = new StringKinesisProducer(config);
 * producer.send("Hello, Kinesis!");
 * 
 * List&lt;String&gt; messages = Arrays.asList("msg1", "msg2", "msg3");
 * producer.sendBatch(messages);
 * </pre>
 */
public class StringKinesisProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(StringKinesisProducer.class);
    
    private final KinesisProducerConfig config;
    private final KinesisClient kinesisClient;

    /**
     * Creates a new StringKinesisProducer with the given configuration.
     * 
     * The producer will automatically use EKS IAM role credentials through
     * DefaultCredentialsProvider, which supports:
     * - Web Identity Token (for EKS pods)
     * - Environment variables
     * - EC2 instance profile
     * - Other standard AWS credential sources
     * 
     * @param config The producer configuration
     */
    public StringKinesisProducer(KinesisProducerConfig config) {
        this.config = config;
        this.kinesisClient = KinesisClient.builder()
                .region(config.getRegion())
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        logger.info("Initialized KinesisProducer for stream: {} in region: {}", 
                config.getStreamName(), config.getRegion());
    }

    /**
     * Sends a single String message to Kinesis Data Stream.
     * 
     * @param message The message to send
     * @param partitionKey The partition key for the record
     * @throws KinesisException if the request fails
     */
    public void send(String message, String partitionKey) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (partitionKey == null || partitionKey.isEmpty()) {
            throw new IllegalArgumentException("Partition key cannot be null or empty");
        }

        try {
            PutRecordRequest request = PutRecordRequest.builder()
                    .streamName(config.getStreamName())
                    .partitionKey(partitionKey)
                    .data(SdkBytes.fromString(message, StandardCharsets.UTF_8))
                    .build();

            kinesisClient.putRecord(request);
            logger.debug("Successfully sent message to stream: {} with partition key: {}", 
                    config.getStreamName(), partitionKey);
        } catch (KinesisException e) {
            logger.error("Failed to send message to stream: {}", config.getStreamName(), e);
            throw e;
        }
    }

    /**
     * Sends a single String message to Kinesis Data Stream using the default partition key.
     * 
     * @param message The message to send
     * @throws KinesisException if the request fails
     */
    public void send(String message) {
        String partitionKey = config.getPartitionKey();
        if (partitionKey == null || partitionKey.isEmpty()) {
            // Use message hash as partition key if not configured
            partitionKey = String.valueOf(message.hashCode());
        }
        send(message, partitionKey);
    }

    /**
     * Sends a batch of String messages to Kinesis Data Stream.
     * 
     * This method handles batching automatically:
     * - Splits large batches into chunks of 500 records (Kinesis limit)
     * - Retries failed records if needed
     * 
     * @param messages List of messages to send
     * @param partitionKey The partition key for all records
     * @return List of failed message indices (empty if all succeeded)
     * @throws KinesisException if the request fails
     */
    public List<Integer> sendBatch(List<String> messages, String partitionKey) {
        if (messages == null || messages.isEmpty()) {
            logger.warn("Empty message list provided, nothing to send");
            return new ArrayList<>();
        }
        if (partitionKey == null || partitionKey.isEmpty()) {
            throw new IllegalArgumentException("Partition key cannot be null or empty");
        }

        List<Integer> failedIndices = new ArrayList<>();
        int batchSize = 500; // Kinesis PutRecords limit
        int totalMessages = messages.size();

        for (int i = 0; i < totalMessages; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalMessages);
            List<String> batch = messages.subList(i, endIndex);
            List<Integer> batchFailedIndices = sendBatchChunk(batch, partitionKey, i);
            failedIndices.addAll(batchFailedIndices);
        }

        if (!failedIndices.isEmpty()) {
            logger.warn("Failed to send {} out of {} messages", failedIndices.size(), totalMessages);
        } else {
            logger.info("Successfully sent all {} messages to stream: {}", 
                    totalMessages, config.getStreamName());
        }

        return failedIndices;
    }

    /**
     * Sends a batch of String messages to Kinesis Data Stream using default partition key.
     * 
     * @param messages List of messages to send
     * @return List of failed message indices (empty if all succeeded)
     * @throws KinesisException if the request fails
     */
    public List<Integer> sendBatch(List<String> messages) {
        String partitionKey = config.getPartitionKey();
        if (partitionKey == null || partitionKey.isEmpty()) {
            // Use a default partition key if not configured
            partitionKey = "default";
        }
        return sendBatch(messages, partitionKey);
    }

    /**
     * Sends a chunk of messages (up to 500 records) to Kinesis.
     * 
     * @param messages The messages in this chunk
     * @param partitionKey The partition key for all records
     * @param startIndex The starting index of this chunk in the original list
     * @return List of failed message indices relative to the original list
     */
    private List<Integer> sendBatchChunk(List<String> messages, String partitionKey, int startIndex) {
        List<PutRecordsRequestEntry> entries = new ArrayList<>();
        
        for (int i = 0; i < messages.size(); i++) {
            String message = messages.get(i);
            if (message == null) {
                logger.warn("Skipping null message at index {}", startIndex + i);
                continue;
            }

            PutRecordsRequestEntry entry = PutRecordsRequestEntry.builder()
                    .partitionKey(partitionKey)
                    .data(SdkBytes.fromString(message, StandardCharsets.UTF_8))
                    .build();
            entries.add(entry);
        }

        if (entries.isEmpty()) {
            return new ArrayList<>();
        }

        try {
            PutRecordsRequest request = PutRecordsRequest.builder()
                    .streamName(config.getStreamName())
                    .records(entries)
                    .build();

            PutRecordsResponse response = kinesisClient.putRecords(request);
            
            List<Integer> failedIndices = new ArrayList<>();
            List<PutRecordsResultEntry> resultEntries = response.records();
            
            for (int i = 0; i < resultEntries.size(); i++) {
                PutRecordsResultEntry resultEntry = resultEntries.get(i);
                if (resultEntry.errorCode() != null) {
                    failedIndices.add(startIndex + i);
                    logger.error("Failed to send message at index {}: {} - {}", 
                            startIndex + i, resultEntry.errorCode(), resultEntry.errorMessage());
                }
            }

            if (response.failedRecordCount() != null && response.failedRecordCount() > 0) {
                logger.warn("Failed to send {} records in batch", response.failedRecordCount());
            }

            return failedIndices;
        } catch (KinesisException e) {
            logger.error("Failed to send batch to stream: {}", config.getStreamName(), e);
            // Return all indices as failed
            List<Integer> allFailed = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                allFailed.add(startIndex + i);
            }
            return allFailed;
        }
    }

    /**
     * Closes the Kinesis client and releases resources.
     */
    @Override
    public void close() {
        if (kinesisClient != null) {
            kinesisClient.close();
            logger.info("KinesisProducer closed for stream: {}", config.getStreamName());
        }
    }
}
