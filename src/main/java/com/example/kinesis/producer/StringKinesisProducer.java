package com.example.kinesis.producer;

import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Kinesis Producer implementation for String data format with batch support.
 */
public class StringKinesisProducer extends BaseKinesisProducer {
    
    public StringKinesisProducer(KinesisProducerConfig config) {
        super(config);
    }
    
    /**
     * Sends a String record to Kinesis Data Stream synchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The String data to send
     * @return PutRecordResponse from Kinesis
     * @throws Exception if the operation fails
     */
    public PutRecordResponse sendString(String streamName, String partitionKey, String data) throws Exception {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return sendRecord(streamName, partitionKey, bytes);
    }
    
    /**
     * Sends a String record to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The String data to send
     * @return CompletableFuture with PutRecordResponse
     */
    public CompletableFuture<PutRecordResponse> sendStringAsync(String streamName, String partitionKey, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return sendRecordAsync(streamName, partitionKey, bytes);
    }
    
    /**
     * Sends multiple String records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to String data
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    public PutRecordsResponse sendStringBatch(String streamName, Map<String, String> records) throws Exception {
        Map<String, byte[]> byteRecords = records.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getBytes(StandardCharsets.UTF_8)
                ));
        return sendBatch(streamName, byteRecords);
    }
    
    /**
     * Sends multiple String records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to String data
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendStringBatchAsync(String streamName, Map<String, String> records) {
        Map<String, byte[]> byteRecords = records.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getBytes(StandardCharsets.UTF_8)
                ));
        return sendBatchAsync(streamName, byteRecords);
    }
    
    /**
     * Sends multiple String records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and String data
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    public PutRecordsResponse sendStringBatch(String streamName, List<StringRecordEntry> records) throws Exception {
        List<RecordEntry> recordEntries = records.stream()
                .map(entry -> new RecordEntry(
                        entry.getPartitionKey(),
                        entry.getData().getBytes(StandardCharsets.UTF_8),
                        entry.getExplicitHashKey()
                ))
                .collect(Collectors.toList());
        return sendBatch(streamName, recordEntries);
    }
    
    /**
     * Sends multiple String records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and String data
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendStringBatchAsync(String streamName, List<StringRecordEntry> records) {
        List<RecordEntry> recordEntries = records.stream()
                .map(entry -> new RecordEntry(
                        entry.getPartitionKey(),
                        entry.getData().getBytes(StandardCharsets.UTF_8),
                        entry.getExplicitHashKey()
                ))
                .collect(Collectors.toList());
        return sendBatchAsync(streamName, recordEntries);
    }
    
    /**
     * Record entry for String batch operations.
     */
    public static class StringRecordEntry {
        private final String partitionKey;
        private final String data;
        private final String explicitHashKey; // Optional
        
        public StringRecordEntry(String partitionKey, String data) {
            this.partitionKey = partitionKey;
            this.data = data;
            this.explicitHashKey = null;
        }
        
        public StringRecordEntry(String partitionKey, String data, String explicitHashKey) {
            this.partitionKey = partitionKey;
            this.data = data;
            this.explicitHashKey = explicitHashKey;
        }
        
        public String getPartitionKey() {
            return partitionKey;
        }
        
        public String getData() {
            return data;
        }
        
        public String getExplicitHashKey() {
            return explicitHashKey;
        }
    }
}
