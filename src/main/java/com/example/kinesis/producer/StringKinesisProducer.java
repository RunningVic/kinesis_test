package com.example.kinesis.producer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Simple Kinesis Producer for sending String messages.
 */
public class StringKinesisProducer {
    
    private final KinesisClient kinesisClient;
    private final KinesisProducerConfig config;
    
    public StringKinesisProducer(KinesisProducerConfig config) {
        this.config = config != null ? config : new KinesisProducerConfig();
        
        // Build Kinesis client
        KinesisClient.Builder builder = KinesisClient.builder();
        
        if (this.config.getRegion() != null) {
            builder.region(this.config.getRegion());
        }
        
        builder.httpClient(ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(10))
                .build());
        
        this.kinesisClient = builder.build();
    }
    
    /**
     * Sends a String message to Kinesis Data Stream.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param message The String message to send
     * @return PutRecordResponse from Kinesis
     * @throws Exception if the operation fails
     */
    public PutRecordResponse send(String streamName, String partitionKey, String message) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data))
                .build();
        
        return kinesisClient.putRecord(request);
    }
    
    /**
     * Closes the producer and releases resources.
     */
    public void close() {
        if (kinesisClient != null) {
            kinesisClient.close();
        }
    }
}
