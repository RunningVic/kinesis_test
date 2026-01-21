package com.example.kinesis.producer;

import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.Map;

/**
 * Interface for synchronous producing records to Kinesis Data Streams.
 * All operations are blocking and return results immediately.
 */
public interface KinesisSyncProducer {
    
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
     * Sends multiple records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to data bytes
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    PutRecordsResponse sendBatch(String streamName, Map<String, byte[]> records) throws Exception;
    
    /**
     * Sends multiple records to Kinesis Data Stream synchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of records, each containing partition key and data
     * @return PutRecordsResponse from Kinesis
     * @throws Exception if the operation fails
     */
    PutRecordsResponse sendBatch(String streamName, List<KinesisProducer.RecordEntry> records) throws Exception;
    
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
}
