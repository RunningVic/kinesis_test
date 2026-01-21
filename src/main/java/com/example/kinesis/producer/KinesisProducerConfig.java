package com.example.kinesis.producer;

import software.amazon.awssdk.regions.Region;

/**
 * Simple configuration class for Kinesis Producer.
 */
public class KinesisProducerConfig {
    private Region region;
    
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
}
