package com.example.kinesis.producer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Basic producer class that contains common functionality for both sync and async producers.
 * This class provides shared utilities for request building, batch processing, and error handling.
 */
public abstract class BasicProducer {
    
    protected final KinesisProducerConfig config;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    
    protected BasicProducer(KinesisProducerConfig config) {
        this.config = config != null ? config : new KinesisProducerConfig();
    }
    
    /**
     * Gets the configuration.
     * 
     * @return The producer configuration
     */
    public KinesisProducerConfig getConfig() {
        return config;
    }
    
    /**
     * Checks if the producer is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
    
    /**
     * Builds a PutRecordRequest from the given parameters.
     * 
     * @param streamName The stream name
     * @param partitionKey The partition key
     * @param data The data bytes
     * @return PutRecordRequest builder (subclasses should call .build())
     */
    protected software.amazon.awssdk.services.kinesis.model.PutRecordRequest.Builder buildPutRecordRequest(
            String streamName, String partitionKey, byte[] data) {
        return software.amazon.awssdk.services.kinesis.model.PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data));
    }
    
    /**
     * Converts a Map of partition keys to data bytes into a list of RecordEntry objects.
     * 
     * @param records Map of partition key to data bytes
     * @return List of RecordEntry objects
     */
    protected List<KinesisProducer.RecordEntry> convertToRecordEntries(Map<String, byte[]> records) {
        return records.entrySet().stream()
                .map(entry -> new KinesisProducer.RecordEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
    
    /**
     * Splits records into batches based on the configured batch size.
     * 
     * @param records List of records to split
     * @return List of batches, each containing at most batchSize records
     */
    protected List<List<KinesisProducer.RecordEntry>> splitIntoBatches(List<KinesisProducer.RecordEntry> records) {
        List<List<KinesisProducer.RecordEntry>> batches = new ArrayList<>();
        for (int i = 0; i < records.size(); i += config.getBatchSize()) {
            int end = Math.min(i + config.getBatchSize(), records.size());
            batches.add(records.subList(i, end));
        }
        return batches;
    }
    
    /**
     * Builds PutRecordsRequestEntry list from RecordEntry list.
     * 
     * @param records List of RecordEntry objects
     * @return List of PutRecordsRequestEntry objects
     */
    protected List<PutRecordsRequestEntry> buildPutRecordsRequestEntries(List<KinesisProducer.RecordEntry> records) {
        return records.stream()
                .map(record -> {
                    PutRecordsRequestEntry.Builder builder = PutRecordsRequestEntry.builder()
                            .partitionKey(record.getPartitionKey())
                            .data(SdkBytes.fromByteArray(record.getData()));
                    
                    if (record.getExplicitHashKey() != null) {
                        builder.explicitHashKey(record.getExplicitHashKey());
                    }
                    
                    return builder.build();
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Builds a PutRecordsRequest from the given parameters.
     * 
     * @param streamName The stream name
     * @param entries List of PutRecordsRequestEntry objects
     * @return PutRecordsRequest builder (subclasses should call .build())
     */
    protected software.amazon.awssdk.services.kinesis.model.PutRecordsRequest.Builder buildPutRecordsRequest(
            String streamName, List<PutRecordsRequestEntry> entries) {
        return software.amazon.awssdk.services.kinesis.model.PutRecordsRequest.builder()
                .streamName(streamName)
                .records(entries);
    }
    
    /**
     * Extracts failed records from a PutRecordsResponse.
     * 
     * @param records Original records that were sent
     * @param response The PutRecordsResponse
     * @return List of failed RecordEntry objects
     */
    protected List<KinesisProducer.RecordEntry> extractFailedRecords(
            List<KinesisProducer.RecordEntry> records,
            software.amazon.awssdk.services.kinesis.model.PutRecordsResponse response) {
        List<KinesisProducer.RecordEntry> failedRecords = new ArrayList<>();
        List<PutRecordsResultEntry> results = response.records();
        
        for (int i = 0; i < results.size(); i++) {
            PutRecordsResultEntry result = results.get(i);
            if (result.errorCode() != null) {
                failedRecords.add(records.get(i));
            }
        }
        
        return failedRecords;
    }
    
    /**
     * Validates that records list is not null or empty.
     * 
     * @param records Records to validate
     * @throws IllegalArgumentException if records is null or empty
     */
    protected void validateRecords(List<KinesisProducer.RecordEntry> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records list cannot be null or empty");
        }
    }
    
    /**
     * Validates that the producer is not closed.
     * 
     * @throws IllegalStateException if producer is closed
     */
    protected void validateNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Producer is closed");
        }
    }
    
    /**
     * Applies common HTTP client configuration for region and endpoint override.
     * This is a helper method for subclasses to configure their clients.
     * 
     * @param region The AWS region
     * @param endpointOverride The endpoint override URL
     * @return URI for endpoint override, or null if not set
     */
    protected URI getEndpointUri(String endpointOverride) {
        return endpointOverride != null ? URI.create(endpointOverride) : null;
    }
    
    /**
     * Gets the connection timeout duration.
     * 
     * @return Duration for connection timeout
     */
    protected Duration getConnectionTimeout() {
        return Duration.ofMillis(config.getRequestTimeoutMillis());
    }
    
    /**
     * Gets the connection time to live duration.
     * 
     * @return Duration for connection TTL
     */
    protected Duration getConnectionTimeToLive() {
        return config.getConnectionTimeToLive();
    }
    
    /**
     * Gets the connection acquisition timeout duration.
     * 
     * @return Duration for connection acquisition timeout
     */
    protected Duration getConnectionAcquisitionTimeout() {
        return Duration.ofSeconds(config.getConnectionAcquisitionTimeoutSeconds());
    }
}
