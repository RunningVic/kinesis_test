package com.example.kinesis.producer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Base implementation for asynchronous Kinesis Producer using native AWS SDK 2.x.
 */
public abstract class BaseKinesisAsyncProducer implements KinesisAsyncProducer {
    
    protected final KinesisAsyncClient kinesisAsyncClient;
    protected final KinesisProducerConfig config;
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public BaseKinesisAsyncProducer(KinesisProducerConfig config) {
        this.config = config != null ? config : new KinesisProducerConfig();
        
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
    }
    
    @Override
    public CompletableFuture<PutRecordResponse> sendRecord(String streamName, String partitionKey, byte[] data) {
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data))
                .build();
        
        return kinesisAsyncClient.putRecord(request);
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatch(String streamName, Map<String, byte[]> records) {
        List<KinesisProducer.RecordEntry> recordEntries = records.entrySet().stream()
                .map(entry -> new KinesisProducer.RecordEntry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        return sendBatch(streamName, recordEntries);
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatch(String streamName, List<KinesisProducer.RecordEntry> records) {
        if (records == null || records.isEmpty()) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Records list cannot be null or empty"));
            return future;
        }
        
        // Split into chunks and send asynchronously
        List<CompletableFuture<PutRecordsResponse>> futures = new ArrayList<>();
        for (int i = 0; i < records.size(); i += config.getBatchSize()) {
            int end = Math.min(i + config.getBatchSize(), records.size());
            List<KinesisProducer.RecordEntry> batch = records.subList(i, end);
            
            CompletableFuture<PutRecordsResponse> future = sendBatchChunk(streamName, batch);
            futures.add(future);
        }
        
        // Combine all futures
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    // Return the last response
                    return futures.get(futures.size() - 1).join();
                });
    }
    
    private CompletableFuture<PutRecordsResponse> sendBatchChunk(String streamName, List<KinesisProducer.RecordEntry> records) {
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
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (kinesisAsyncClient != null) {
                kinesisAsyncClient.close();
            }
        }
    }
}
