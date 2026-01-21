package com.example.kinesis.producer;

import software.amazon.awssdk.regions.Region;

/**
 * Factory class for creating Kinesis Producer instances (both sync and async).
 */
public class KinesisProducerFactory {
    
    // ========== String Sync Producers ==========
    
    /**
     * Creates a StringKinesisProducer (synchronous) with default configuration.
     * 
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer() {
        return new StringKinesisProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates a StringKinesisProducer (synchronous) with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer(KinesisProducerConfig config) {
        return new StringKinesisProducer(config);
    }
    
    /**
     * Creates a StringKinesisProducer (synchronous) with region.
     * 
     * @param region The AWS region
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new StringKinesisProducer(config);
    }
    
    // ========== String Async Producers ==========
    
    /**
     * Creates a StringKinesisAsyncProducer (asynchronous) with default configuration.
     * 
     * @return StringKinesisAsyncProducer instance
     */
    public static StringKinesisAsyncProducer createStringAsyncProducer() {
        return new StringKinesisAsyncProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates a StringKinesisAsyncProducer (asynchronous) with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return StringKinesisAsyncProducer instance
     */
    public static StringKinesisAsyncProducer createStringAsyncProducer(KinesisProducerConfig config) {
        return new StringKinesisAsyncProducer(config);
    }
    
    /**
     * Creates a StringKinesisAsyncProducer (asynchronous) with region.
     * 
     * @param region The AWS region
     * @return StringKinesisAsyncProducer instance
     */
    public static StringKinesisAsyncProducer createStringAsyncProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new StringKinesisAsyncProducer(config);
    }
    
    // ========== AVRO Sync Producers ==========
    
    /**
     * Creates an AvroKinesisProducer (synchronous) with default configuration.
     * 
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer() {
        return new AvroKinesisProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates an AvroKinesisProducer (synchronous) with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer(KinesisProducerConfig config) {
        return new AvroKinesisProducer(config);
    }
    
    /**
     * Creates an AvroKinesisProducer (synchronous) with region.
     * 
     * @param region The AWS region
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new AvroKinesisProducer(config);
    }
    
    // ========== AVRO Async Producers ==========
    
    /**
     * Creates an AvroKinesisAsyncProducer (asynchronous) with default configuration.
     * 
     * @return AvroKinesisAsyncProducer instance
     */
    public static AvroKinesisAsyncProducer createAvroAsyncProducer() {
        return new AvroKinesisAsyncProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates an AvroKinesisAsyncProducer (asynchronous) with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return AvroKinesisAsyncProducer instance
     */
    public static AvroKinesisAsyncProducer createAvroAsyncProducer(KinesisProducerConfig config) {
        return new AvroKinesisAsyncProducer(config);
    }
    
    /**
     * Creates an AvroKinesisAsyncProducer (asynchronous) with region.
     * 
     * @param region The AWS region
     * @return AvroKinesisAsyncProducer instance
     */
    public static AvroKinesisAsyncProducer createAvroAsyncProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new AvroKinesisAsyncProducer(config);
    }
}
