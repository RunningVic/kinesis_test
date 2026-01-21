package com.example.kinesis.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous Kinesis Producer implementation for AVRO data format.
 */
public class AvroKinesisAsyncProducer extends BaseKinesisAsyncProducer {
    
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    
    public AvroKinesisAsyncProducer(KinesisProducerConfig config) {
        super(config);
    }
    
    /**
     * Sends a GenericRecord (AVRO) to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param record The AVRO GenericRecord to send
     * @return CompletableFuture with PutRecordResponse
     */
    public CompletableFuture<PutRecordResponse> sendAvro(String streamName, String partitionKey, GenericRecord record) {
        try {
            byte[] avroBytes = serializeAvro(record);
            return sendRecord(streamName, partitionKey, avroBytes);
        } catch (Exception e) {
            CompletableFuture<PutRecordResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    /**
     * Sends a SpecificRecord (AVRO) to Kinesis Data Stream asynchronously.
     * 
     * @param streamName The name of the Kinesis stream
     * @param partitionKey The partition key for the record
     * @param record The AVRO SpecificRecord to send
     * @return CompletableFuture with PutRecordResponse
     */
    public CompletableFuture<PutRecordResponse> sendAvro(String streamName, String partitionKey, SpecificRecord record) {
        try {
            byte[] avroBytes = serializeAvro(record);
            return sendRecord(streamName, partitionKey, avroBytes);
        } catch (Exception e) {
            CompletableFuture<PutRecordResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    /**
     * Serializes an AVRO record to byte array.
     * 
     * @param record The AVRO record to serialize
     * @return Serialized byte array
     * @throws IOException if serialization fails
     */
    private byte[] serializeAvro(GenericRecord record) throws IOException {
        Schema schema = record.getSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = encoderFactory.binaryEncoder(outputStream, null);
        
        datumWriter.write(record, encoder);
        encoder.flush();
        
        return outputStream.toByteArray();
    }
    
    /**
     * Serializes an AVRO SpecificRecord to byte array.
     * 
     * @param record The AVRO SpecificRecord to serialize
     * @return Serialized byte array
     * @throws IOException if serialization fails
     */
    private byte[] serializeAvro(SpecificRecord record) throws IOException {
        Schema schema = record.getSchema();
        DatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>(schema);
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = encoderFactory.binaryEncoder(outputStream, null);
        
        datumWriter.write(record, encoder);
        encoder.flush();
        
        return outputStream.toByteArray();
    }
    
    /**
     * Sends multiple GenericRecord (AVRO) records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to AVRO GenericRecord
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendGenericRecordBatch(String streamName, Map<String, GenericRecord> records) {
        try {
            List<KinesisProducer.RecordEntry> recordEntries = new ArrayList<>();
            for (Map.Entry<String, GenericRecord> entry : records.entrySet()) {
                byte[] avroBytes = serializeAvro(entry.getValue());
                recordEntries.add(new KinesisProducer.RecordEntry(entry.getKey(), avroBytes));
            }
            return sendBatch(streamName, recordEntries);
        } catch (Exception e) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    /**
     * Sends multiple SpecificRecord (AVRO) records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records Map of partition key to AVRO SpecificRecord
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendSpecificRecordBatch(String streamName, Map<String, SpecificRecord> records) {
        try {
            List<KinesisProducer.RecordEntry> recordEntries = new ArrayList<>();
            for (Map.Entry<String, SpecificRecord> entry : records.entrySet()) {
                byte[] avroBytes = serializeAvro(entry.getValue());
                recordEntries.add(new KinesisProducer.RecordEntry(entry.getKey(), avroBytes));
            }
            return sendBatch(streamName, recordEntries);
        } catch (Exception e) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    /**
     * Sends multiple AVRO records to Kinesis Data Stream asynchronously using batch API.
     * 
     * @param streamName The name of the Kinesis stream
     * @param records List of AVRO record entries
     * @return CompletableFuture with PutRecordsResponse
     */
    public CompletableFuture<PutRecordsResponse> sendAvroBatch(String streamName, List<AvroRecordEntry> records) {
        try {
            List<KinesisProducer.RecordEntry> recordEntries = new ArrayList<>();
            for (AvroRecordEntry entry : records) {
                byte[] avroBytes;
                if (entry.getGenericRecord() != null) {
                    avroBytes = serializeAvro(entry.getGenericRecord());
                } else if (entry.getSpecificRecord() != null) {
                    avroBytes = serializeAvro(entry.getSpecificRecord());
                } else {
                    throw new IllegalArgumentException("Record entry must contain either GenericRecord or SpecificRecord");
                }
                recordEntries.add(new KinesisProducer.RecordEntry(entry.getPartitionKey(), avroBytes, entry.getExplicitHashKey()));
            }
            return sendBatch(streamName, recordEntries);
        } catch (Exception e) {
            CompletableFuture<PutRecordsResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }
    
    /**
     * Record entry for AVRO batch operations.
     */
    public static class AvroRecordEntry {
        private final String partitionKey;
        private final GenericRecord genericRecord;
        private final SpecificRecord specificRecord;
        private final String explicitHashKey; // Optional
        
        public AvroRecordEntry(String partitionKey, GenericRecord record) {
            this.partitionKey = partitionKey;
            this.genericRecord = record;
            this.specificRecord = null;
            this.explicitHashKey = null;
        }
        
        public AvroRecordEntry(String partitionKey, SpecificRecord record) {
            this.partitionKey = partitionKey;
            this.genericRecord = null;
            this.specificRecord = record;
            this.explicitHashKey = null;
        }
        
        public AvroRecordEntry(String partitionKey, GenericRecord record, String explicitHashKey) {
            this.partitionKey = partitionKey;
            this.genericRecord = record;
            this.specificRecord = null;
            this.explicitHashKey = explicitHashKey;
        }
        
        public AvroRecordEntry(String partitionKey, SpecificRecord record, String explicitHashKey) {
            this.partitionKey = partitionKey;
            this.genericRecord = null;
            this.specificRecord = record;
            this.explicitHashKey = explicitHashKey;
        }
        
        public String getPartitionKey() {
            return partitionKey;
        }
        
        public GenericRecord getGenericRecord() {
            return genericRecord;
        }
        
        public SpecificRecord getSpecificRecord() {
            return specificRecord;
        }
        
        public String getExplicitHashKey() {
            return explicitHashKey;
        }
    }
}
