package com.example.kinesis.producer;

import software.amazon.awssdk.regions.Region;

/**
 * Factory class for creating Kinesis Producer instances.
 */
public class KinesisProducerFactory {
    
    /**
     * Creates a StringKinesisProducer with default configuration.
     * 
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer() {
        return new StringKinesisProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates a StringKinesisProducer with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer(KinesisProducerConfig config) {
        return new StringKinesisProducer(config);
    }
    
    /**
     * Creates a StringKinesisProducer with region.
     * 
     * @param region The AWS region
     * @return StringKinesisProducer instance
     */
    public static StringKinesisProducer createStringProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new StringKinesisProducer(config);
    }
    
    /**
     * Creates an AvroKinesisProducer with default configuration.
     * 
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer() {
        return new AvroKinesisProducer(new KinesisProducerConfig());
    }
    
    /**
     * Creates an AvroKinesisProducer with custom configuration.
     * 
     * @param config The configuration for the producer
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer(KinesisProducerConfig config) {
        return new AvroKinesisProducer(config);
    }
    
    /**
     * Creates an AvroKinesisProducer with region.
     * 
     * @param region The AWS region
     * @return AvroKinesisProducer instance
     */
    public static AvroKinesisProducer createAvroProducer(Region region) {
        KinesisProducerConfig config = new KinesisProducerConfig().setRegion(region);
        return new AvroKinesisProducer(config);
    }
}
