package com.example.kinesis.producer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Base implementation of KinesisProducer using native AWS SDK 2.x with batch support and performance optimizations.
 */
public abstract class BaseKinesisProducer implements KinesisProducer {
    
    protected final KinesisClient kinesisClient;
    protected final KinesisAsyncClient kinesisAsyncClient;
    protected final KinesisProducerConfig config;
    
    // Batch buffering
    private final Map<String, List<RecordEntry>> batchBuffers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushScheduler;
    private final ExecutorService asyncExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public BaseKinesisProducer(KinesisProducerConfig config) {
        this.config = config != null ? config : new KinesisProducerConfig();
        
        // Build synchronous client with performance optimizations
        KinesisClient.Builder syncBuilder = KinesisClient.builder();
        if (this.config.getRegion() != null) {
            syncBuilder.region(this.config.getRegion());
        }
        if (this.config.getEndpointOverride() != null) {
            syncBuilder.endpointOverride(URI.create(this.config.getEndpointOverride()));
        }
        
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(this.config.getMaxConnections())
                .connectionTimeout(Duration.ofMillis(this.config.getRequestTimeoutMillis()))
                .connectionTimeToLive(this.config.getConnectionTimeToLive())
                .connectionAcquisitionTimeout(Duration.ofSeconds(this.config.getConnectionAcquisitionTimeoutSeconds()));
        
        if (this.config.isEnableConnectionPooling()) {
            httpClientBuilder.useIdleConnectionReaper(true);
        }
        
        syncBuilder.httpClient(httpClientBuilder.build());
        this.kinesisClient = syncBuilder.build();
        
        // Build asynchronous client with performance optimizations
        KinesisAsyncClient.Builder asyncBuilder = KinesisAsyncClient.builder();
        if (this.config.getRegion() != null) {
            asyncBuilder.region(this.config.getRegion());
        }
        if (this.config.getEndpointOverride() != null) {
            asyncBuilder.endpointOverride(URI.create(this.config.getEndpointOverride()));
        }
        
        NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(this.config.getMaxConnections())
                .connectionTimeout(Duration.ofMillis(this.config.getRequestTimeoutMillis()))
                .connectionTimeToLive(this.config.getConnectionTimeToLive())
                .connectionAcquisitionTimeout(Duration.ofSeconds(this.config.getConnectionAcquisitionTimeoutSeconds()));
        
        if (this.config.isEnableConnectionPooling()) {
            asyncHttpClientBuilder.useIdleConnectionReaper(true);
        }
        
        asyncBuilder.httpClient(asyncHttpClientBuilder.build());
        this.kinesisAsyncClient = asyncBuilder.build();
        
        // Initialize thread pools for async operations and batch flushing
        this.asyncExecutor = Executors.newFixedThreadPool(
                this.config.getThreadPoolSize(),
                r -> {
                    Thread t = new Thread(r, "kinesis-producer-async-" + System.currentTimeMillis());
                    t.setDaemon(true);
                    return t;
                }
        );
        
        // Start scheduled flush if auto-flush is enabled
        if (this.config.isEnableAutoFlush()) {
            this.flushScheduler = Executors.newScheduledThreadPool(
                    1,
                    r -> {
                        Thread t = new Thread(r, "kinesis-producer-flush");
                        t.setDaemon(true);
                        return t;
                    }
            );
            this.flushScheduler.scheduleAtFixedRate(
                    this::flushAllBuffers,
                    this.config.getBatchFlushIntervalMillis(),
                    this.config.getBatchFlushIntervalMillis(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            this.flushScheduler = null;
        }
    }
    
    @Override
    public PutRecordResponse sendRecord(String streamName, String partitionKey, byte[] data) throws Exception {
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data))
                .build();
        
        return kinesisClient.putRecord(request);
    }
    
    @Override
    public CompletableFuture<PutRecordResponse> sendRecordAsync(String streamName, String partitionKey, byte[] data) {
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data))
                .build();
        
        return kinesisAsyncClient.putRecord(request);
    }
    
    @Override
    public PutRecordsResponse sendBatch(String streamName, Map<String, byte[]> records) throws Exception {
        List<RecordEntry> recordEntries = records.entrySet().stream()
                .map(entry -> new RecordEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        return sendBatch(streamName, recordEntries);
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatchAsync(String streamName, Map<String, byte[]> records) {
        List<RecordEntry> recordEntries = records.entrySet().stream()
                .map(entry -> new RecordEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        return sendBatchAsync(streamName, recordEntries);
    }
    
    @Override
    public PutRecordsResponse sendBatch(String streamName, List<RecordEntry> records) throws Exception {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records list cannot be null or empty");
        }
        
        // Split into chunks if exceeds batch size limit
        List<PutRecordsResponse> responses = new ArrayList<>();
        for (int i = 0; i < records.size(); i += config.getBatchSize()) {
            int end = Math.min(i + config.getBatchSize(), records.size());
            List<RecordEntry> batch = records.subList(i, end);
            
            PutRecordsResponse response = sendBatchChunk(streamName, batch);
            responses.add(response);
            
            // Handle failed records if any
            if (response.failedRecordCount() != null && response.failedRecordCount() > 0) {
                handleFailedRecords(streamName, batch, response);
            }
        }
        
        // Return the last response (or merge if needed)
        return responses.get(responses.size() - 1);
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatchAsync(String streamName, List<RecordEntry> records) {
        if (records == null || records.isEmpty()) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Records list cannot be null or empty"));
            return future;
        }
        
        // Split into chunks and send asynchronously
        List<CompletableFuture<PutRecordsResponse>> futures = new ArrayList<>();
        for (int i = 0; i < records.size(); i += config.getBatchSize()) {
            int end = Math.min(i + config.getBatchSize(), records.size());
            List<RecordEntry> batch = records.subList(i, end);
            
            CompletableFuture<PutRecordsResponse> future = sendBatchChunkAsync(streamName, batch);
            futures.add(future);
        }
        
        // Combine all futures
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    // Return the last response
                    return futures.get(futures.size() - 1).join();
                });
    }
    
    private PutRecordsResponse sendBatchChunk(String streamName, List<RecordEntry> records) throws Exception {
        List<PutRecordsRequestEntry> entries = records.stream()
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
        
        PutRecordsRequest request = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(entries)
                .build();
        
        return kinesisClient.putRecords(request);
    }
    
    private CompletableFuture<PutRecordsResponse> sendBatchChunkAsync(String streamName, List<RecordEntry> records) {
        List<PutRecordsRequestEntry> entries = records.stream()
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
        
        PutRecordsRequest request = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(entries)
                .build();
        
        return kinesisAsyncClient.putRecords(request);
    }
    
    private void handleFailedRecords(String streamName, List<RecordEntry> records, PutRecordsResponse response) {
        // Retry failed records
        List<RecordEntry> failedRecords = new ArrayList<>();
        List<PutRecordsResultEntry> results = response.records();
        
        for (int i = 0; i < results.size(); i++) {
            PutRecordsResultEntry result = results.get(i);
            if (result.errorCode() != null) {
                failedRecords.add(records.get(i));
            }
        }
        
        // Retry failed records (simple retry, could be enhanced with exponential backoff)
        if (!failedRecords.isEmpty() && config.getMaxRetries() > 0) {
            try {
                Thread.sleep(100); // Simple delay before retry
                sendBatch(streamName, failedRecords);
            } catch (Exception e) {
                // Log error or handle as needed
                System.err.println("Failed to retry records: " + e.getMessage());
            }
        }
    }
    
    @Override
    public void flush() throws Exception {
        flushAllBuffers();
    }
    
    private void flushAllBuffers() {
        if (closed.get()) {
            return;
        }
        
        for (Map.Entry<String, List<RecordEntry>> entry : batchBuffers.entrySet()) {
            String streamName = entry.getKey();
            List<RecordEntry> records = entry.getValue();
            
            if (!records.isEmpty()) {
                synchronized (records) {
                    if (!records.isEmpty()) {
                        List<RecordEntry> toFlush = new ArrayList<>(records);
                        records.clear();
                        
                        try {
                            sendBatch(streamName, toFlush);
                        } catch (Exception e) {
                            // Log error or handle as needed
                            System.err.println("Failed to flush batch for stream " + streamName + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Adds a record to the batch buffer for the specified stream.
     * The record will be automatically flushed when the buffer reaches the batch size
     * or when the flush interval elapses.
     */
    protected void addToBatchBuffer(String streamName, RecordEntry record) {
        if (closed.get()) {
            throw new IllegalStateException("Producer is closed");
        }
        
        batchBuffers.computeIfAbsent(streamName, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(record);
        
        // Check if buffer should be flushed immediately
        List<RecordEntry> buffer = batchBuffers.get(streamName);
        synchronized (buffer) {
            if (buffer.size() >= config.getBatchSize()) {
                List<RecordEntry> toFlush = new ArrayList<>(buffer);
                buffer.clear();
                
                // Flush asynchronously to avoid blocking
                asyncExecutor.submit(() -> {
                    try {
                        sendBatch(streamName, toFlush);
                    } catch (Exception e) {
                        System.err.println("Failed to flush batch: " + e.getMessage());
                    }
                });
            }
        }
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Flush all pending records
            try {
                flushAllBuffers();
            } catch (Exception e) {
                System.err.println("Error during final flush: " + e.getMessage());
            }
            
            // Shutdown executors
            if (flushScheduler != null) {
                flushScheduler.shutdown();
                try {
                    if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        flushScheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    flushScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            // Close clients
            if (kinesisClient != null) {
                kinesisClient.close();
            }
            if (kinesisAsyncClient != null) {
                kinesisAsyncClient.close();
            }
        }
    }
}
