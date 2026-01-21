package com.example.kinesis.producer;

import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for asynchronous producing records to Kinesis Data Streams.
 * All operations are non-blocking and return CompletableFuture.
 */
public interface KinesisAsyncProducer {
    
    /**
     * Sends a record to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param data The data to send
     * @return CompletableFuture with PutRecordResponse
     */
    CompletableFuture<PutRecordResponse> sendRecord(String streamName, String partitionKey, byte[] data);
    
    /**
     * Sends multiple records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to data bytes
     * @return CompletableFuture with PutRecordsResponse
     */
    CompletableFuture<PutRecordsResponse> sendBatch(String streamName, Map<String, byte[]> records);
    
    /**
     * Sends multiple records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and data
     * @return CompletableFuture with PutRecordsResponse
     */
    CompletableFuture<PutRecordsResponse> sendBatch(String streamName, List<KinesisProducer.RecordEntry> records);
    
    /**
     * Closes the producer and releases resources.
     */
    void close();
}
