package com.example.kinesis.producer;

import software.amazon.awssdk.regions.Region;

import java.time.Duration;

/**
 * Configuration class for Kinesis Producer with batch and performance optimization options.
 */
public class KinesisProducerConfig {
    private Region region;
    private String endpointOverride;
    private int maxConnections = 50; // Increased for better throughput
    private long requestTimeoutMillis = 60000;
    private int maxRetries = 3;
    
    // Batch configuration
    private int batchSize = 500; // Max records per batch (Kinesis limit is 500)
    private long batchFlushIntervalMillis = 1000; // Auto-flush interval
    private int maxBatchSizeBytes = 5 * 1024 * 1024; // 5MB max batch size
    private boolean enableAutoFlush = true; // Enable automatic batch flushing
    
    // Performance optimization
    private int threadPoolSize = 10; // Thread pool size for async operations
    private boolean enableConnectionPooling = true;
    private Duration connectionTimeToLive = Duration.ofMinutes(5);
    private int connectionAcquisitionTimeoutSeconds = 60;
    
    public KinesisProducerConfig() {
        // Default constructor
    }
    
    public Region getRegion() {
        return region;
    }
    
    public KinesisProducerConfig setRegion(Region region) {
        this.region = region;
        return this;
    }
    
    public String getEndpointOverride() {
        return endpointOverride;
    }
    
    public KinesisProducerConfig setEndpointOverride(String endpointOverride) {
        this.endpointOverride = endpointOverride;
        return this;
    }
    
    public int getMaxConnections() {
        return maxConnections;
    }
    
    public KinesisProducerConfig setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }
    
    public long getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }
    
    public KinesisProducerConfig setRequestTimeoutMillis(long requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
        return this;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public KinesisProducerConfig setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public KinesisProducerConfig setBatchSize(int batchSize) {
        if (batchSize > 500) {
            throw new IllegalArgumentException("Batch size cannot exceed 500 (Kinesis limit)");
        }
        this.batchSize = batchSize;
        return this;
    }
    
    public long getBatchFlushIntervalMillis() {
        return batchFlushIntervalMillis;
    }
    
    public KinesisProducerConfig setBatchFlushIntervalMillis(long batchFlushIntervalMillis) {
        this.batchFlushIntervalMillis = batchFlushIntervalMillis;
        return this;
    }
    
    public int getMaxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }
    
    public KinesisProducerConfig setMaxBatchSizeBytes(int maxBatchSizeBytes) {
        if (maxBatchSizeBytes > 5 * 1024 * 1024) {
            throw new IllegalArgumentException("Max batch size cannot exceed 5MB (Kinesis limit)");
        }
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        return this;
    }
    
    public boolean isEnableAutoFlush() {
        return enableAutoFlush;
    }
    
    public KinesisProducerConfig setEnableAutoFlush(boolean enableAutoFlush) {
        this.enableAutoFlush = enableAutoFlush;
        return this;
    }
    
    public int getThreadPoolSize() {
        return threadPoolSize;
    }
    
    public KinesisProducerConfig setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        return this;
    }
    
    public boolean isEnableConnectionPooling() {
        return enableConnectionPooling;
    }
    
    public KinesisProducerConfig setEnableConnectionPooling(boolean enableConnectionPooling) {
        this.enableConnectionPooling = enableConnectionPooling;
        return this;
    }
    
    public Duration getConnectionTimeToLive() {
        return connectionTimeToLive;
    }
    
    public KinesisProducerConfig setConnectionTimeToLive(Duration connectionTimeToLive) {
        this.connectionTimeToLive = connectionTimeToLive;
        return this;
    }
    
    public int getConnectionAcquisitionTimeoutSeconds() {
        return connectionAcquisitionTimeoutSeconds;
    }
    
    public KinesisProducerConfig setConnectionAcquisitionTimeoutSeconds(int connectionAcquisitionTimeoutSeconds) {
        this.connectionAcquisitionTimeoutSeconds = connectionAcquisitionTimeoutSeconds;
        return this;
    }
}
