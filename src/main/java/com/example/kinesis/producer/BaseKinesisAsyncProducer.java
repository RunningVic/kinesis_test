package com.example.kinesis.producer;

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Base implementation for asynchronous Kinesis Producer using native AWS SDK 2.x.
 * Extends BasicProducer to reuse common functionality.
 */
public abstract class BaseKinesisAsyncProducer extends BasicProducer implements KinesisAsyncProducer {
    
    protected final KinesisAsyncClient kinesisAsyncClient;
    
    public BaseKinesisAsyncProducer(KinesisProducerConfig config) {
        super(config);
        
        // Build asynchronous client with performance optimizations
        KinesisAsyncClient.Builder asyncBuilder = KinesisAsyncClient.builder();
        if (config.getRegion() != null) {
            asyncBuilder.region(config.getRegion());
        }
        
        URI endpointUri = getEndpointUri(config.getEndpointOverride());
        if (endpointUri != null) {
            asyncBuilder.endpointOverride(endpointUri);
        }
        
        NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(config.getMaxConnections())
                .connectionTimeout(getConnectionTimeout())
                .connectionTimeToLive(getConnectionTimeToLive())
                .connectionAcquisitionTimeout(getConnectionAcquisitionTimeout());
        
        if (config.isEnableConnectionPooling()) {
            asyncHttpClientBuilder.useIdleConnectionReaper(true);
        }
        
        asyncBuilder.httpClient(asyncHttpClientBuilder.build());
        this.kinesisAsyncClient = asyncBuilder.build();
    }
    
    @Override
    public CompletableFuture<PutRecordResponse> sendRecord(String streamName, String partitionKey, byte[] data) {
        validateNotClosed();
        return kinesisAsyncClient.putRecord(
                buildPutRecordRequest(streamName, partitionKey, data).build()
        );
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatch(String streamName, Map<String, byte[]> records) {
        return sendBatch(streamName, convertToRecordEntries(records));
    }
    
    @Override
    public CompletableFuture<PutRecordsResponse> sendBatch(String streamName, List<KinesisProducer.RecordEntry> records) {
        validateNotClosed();
        
        if (records == null || records.isEmpty()) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Records list cannot be null or empty"));
            return future;
        }
        
        // Split into chunks and send asynchronously
        List<CompletableFuture<PutRecordsResponse>> futures = new ArrayList<>();
        List<List<KinesisProducer.RecordEntry>> batches = splitIntoBatches(records);
        
        for (List<KinesisProducer.RecordEntry> batch : batches) {
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
        List<PutRecordsRequestEntry> entries = buildPutRecordsRequestEntries(records);
        return kinesisAsyncClient.putRecords(
                buildPutRecordsRequest(streamName, entries).build()
        );
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
