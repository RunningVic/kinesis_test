package com.example.kinesis.producer;

import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for producing records to Kinesis Data Streams.
 * Supports both synchronous and asynchronous operations, including batch operations.
 * 
 * This implementation uses native AWS SDK 2.x Kinesis clients.
 */
public interface KinesisProducer {
    
    /**
     * Sends a record to Kinesis Data Stream synchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The data to send
     * @return PutRecordResponse from Kinesis
     * @throws Exception if the operation fails
     */
    PutRecordResponse sendRecord(String streamName, String partitionKey, byte[] data) throws Exception;
    
    /**
     * Sends a record to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The data to send
     * @return CompletableFuture with PutRecordResponse
     */
    CompletableFuture<PutRecordResponse> sendRecordAsync(String streamName, String partitionKey, byte[] data);
    
    /**
     * Sends multiple records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to data bytes
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    PutRecordsResponse sendBatch(String streamName, Map<String, byte[]> records) throws Exception;
    
    /**
     * Sends multiple records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to data bytes
     * @return CompletableFuture with PutRecordsResponse
     */
    CompletableFuture<PutRecordsResponse> sendBatchAsync(String streamName, Map<String, byte[]> records);
    
    /**
     * Sends multiple records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and data
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    PutRecordsResponse sendBatch(String streamName, List<RecordEntry> records) throws Exception;
    
    /**
     * Sends multiple records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and data
     * @return CompletableFuture with PutRecordsResponse
     */
    CompletableFuture<PutRecordsResponse> sendBatchAsync(String streamName, List<RecordEntry> records);
    
    /**
     * Flushes any buffered records immediately.
     * 
     * @throws Exception if the operation fails
     */
    void flush() throws Exception;
    
    /**
     * Closes the producer and releases resources.
     * This will flush any pending records before closing.
     */
    void close();
    
    /**
     * Record entry for batch operations.
     */
    class RecordEntry {
        private final String partitionKey;
        private final byte[] data;
        private final String explicitHashKey; // Optional
        
        public RecordEntry(String partitionKey, byte[] data) {
            this.partitionKey = partitionKey;
            this.data = data;
            this.explicitHashKey = null;
        }
        
        public RecordEntry(String partitionKey, byte[] data, String explicitHashKey) {
            this.partitionKey = partitionKey;
            this.data = data;
            this.explicitHashKey = explicitHashKey;
        }
        
        public String getPartitionKey() {
            return partitionKey;
        }
        
        public byte[] getData() {
            return data;
        }
        
        public String getExplicitHashKey() {
            return explicitHashKey;
        }
    }
}
