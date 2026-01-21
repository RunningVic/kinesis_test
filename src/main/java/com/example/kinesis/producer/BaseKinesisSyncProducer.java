package com.example.kinesis.producer;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base implementation for synchronous Kinesis Producer using native AWS SDK 2.x.
 * Extends BasicProducer to reuse common functionality.
 */
public abstract class BaseKinesisSyncProducer extends BasicProducer implements KinesisSyncProducer {
    
    protected final KinesisClient kinesisClient;
    
    // Batch buffering
    private final Map<String, List<KinesisProducer.RecordEntry>> batchBuffers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushScheduler;
    
    public BaseKinesisSyncProducer(KinesisProducerConfig config) {
        super(config);
        
        // Build synchronous client with performance optimizations
        KinesisClient.Builder syncBuilder = KinesisClient.builder();
        if (config.getRegion() != null) {
            syncBuilder.region(config.getRegion());
        }
        
        URI endpointUri = getEndpointUri(config.getEndpointOverride());
        if (endpointUri != null) {
            syncBuilder.endpointOverride(endpointUri);
        }
        
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections())
                .connectionTimeout(getConnectionTimeout())
                .connectionTimeToLive(getConnectionTimeToLive())
                .connectionAcquisitionTimeout(getConnectionAcquisitionTimeout());
        
        if (config.isEnableConnectionPooling()) {
            httpClientBuilder.useIdleConnectionReaper(true);
        }
        
        syncBuilder.httpClient(httpClientBuilder.build());
        this.kinesisClient = syncBuilder.build();
        
        // Start scheduled flush if auto-flush is enabled
        if (config.isEnableAutoFlush()) {
            this.flushScheduler = Executors.newScheduledThreadPool(
                    1,
                    r -> {
                        Thread t = new Thread(r, "kinesis-sync-producer-flush");
                        t.setDaemon(true);
                        return t;
                    }
            );
            this.flushScheduler.scheduleAtFixedRate(
                    this::flushAllBuffers,
                    config.getBatchFlushIntervalMillis(),
                    config.getBatchFlushIntervalMillis(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            this.flushScheduler = null;
        }
    }
    
    @Override
    public PutRecordResponse sendRecord(String streamName, String partitionKey, byte[] data) throws Exception {
        validateNotClosed();
        return kinesisClient.putRecord(
                buildPutRecordRequest(streamName, partitionKey, data).build()
        );
    }
    
    @Override
    public PutRecordsResponse sendBatch(String streamName, Map<String, byte[]> records) throws Exception {
        return sendBatch(streamName, convertToRecordEntries(records));
    }
    
    @Override
    public PutRecordsResponse sendBatch(String streamName, List<KinesisProducer.RecordEntry> records) throws Exception {
        validateNotClosed();
        validateRecords(records);
        
        // Split into chunks if exceeds batch size limit
        List<PutRecordsResponse> responses = new ArrayList<>();
        List<List<KinesisProducer.RecordEntry>> batches = splitIntoBatches(records);
        
        for (List<KinesisProducer.RecordEntry> batch : batches) {
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
    
    private PutRecordsResponse sendBatchChunk(String streamName, List<KinesisProducer.RecordEntry> records) throws Exception {
        List<PutRecordsRequestEntry> entries = buildPutRecordsRequestEntries(records);
        return kinesisClient.putRecords(
                buildPutRecordsRequest(streamName, entries).build()
        );
    }
    
    private void handleFailedRecords(String streamName, List<KinesisProducer.RecordEntry> records, PutRecordsResponse response) {
        List<KinesisProducer.RecordEntry> failedRecords = extractFailedRecords(records, response);
        
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
        validateNotClosed();
        flushAllBuffers();
    }
    
    private void flushAllBuffers() {
        if (isClosed()) {
            return;
        }
        
        for (Map.Entry<String, List<KinesisProducer.RecordEntry>> entry : batchBuffers.entrySet()) {
            String streamName = entry.getKey();
            List<KinesisProducer.RecordEntry> records = entry.getValue();
            
            if (!records.isEmpty()) {
                synchronized (records) {
                    if (!records.isEmpty()) {
                        List<KinesisProducer.RecordEntry> toFlush = new ArrayList<>(records);
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
     */
    protected void addToBatchBuffer(String streamName, KinesisProducer.RecordEntry record) {
        validateNotClosed();
        
        batchBuffers.computeIfAbsent(streamName, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(record);
        
        // Check if buffer should be flushed immediately
        List<KinesisProducer.RecordEntry> buffer = batchBuffers.get(streamName);
        synchronized (buffer) {
            if (buffer.size() >= config.getBatchSize()) {
                List<KinesisProducer.RecordEntry> toFlush = new ArrayList<>(buffer);
                buffer.clear();
                
                // Flush synchronously
                try {
                    sendBatch(streamName, toFlush);
                } catch (Exception e) {
                    System.err.println("Failed to flush batch: " + e.getMessage());
                }
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
            
            // Shutdown scheduler
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
            
            // Close client
            if (kinesisClient != null) {
                kinesisClient.close();
            }
        }
    }
}
