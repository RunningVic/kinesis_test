package com.example.kinesis.producer;

import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Asynchronous Kinesis Producer implementation for String data format.
 */
public class StringKinesisAsyncProducer extends BaseKinesisAsyncProducer {
    
    public StringKinesisAsyncProducer(KinesisProducerConfig config) {
        super(config);
    }
    
    /**
     * Sends a String record to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The String data to send
     * @return CompletableFuture with PutRecordResponse
     */
    public CompletableFuture<PutRecordResponse> sendString(String streamName, String partitionKey, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return sendRecord(streamName, partitionKey, bytes);
    }
    
    /**
     * Sends multiple String records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to String data
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendStringBatch(String streamName, Map<String, String> records) {
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
     * @param records List of records, each containing partition key and String data
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendStringBatch(String streamName, List<StringRecordEntry> records) {
        List<KinesisProducer.RecordEntry> recordEntries = records.stream()
                .map(entry -> new KinesisProducer.RecordEntry(
                        entry.getPartitionKey(),
                        entry.getData().getBytes(StandardCharsets.UTF_8),
                        entry.getExplicitHashKey()
                ))
                .collect(Collectors.toList());
        return sendBatch(streamName, recordEntries);
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
