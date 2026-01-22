package com.example.kinesis.producer;

import software.amazon.awssdk.regions.Region;

/**
 * Configuration class for Kinesis Producer.
 * This class holds configuration parameters for the Kinesis producer.
 */
public class KinesisProducerConfig {
    private String streamName;
    private Region region;
    private String partitionKey;

    private KinesisProducerConfig(Builder builder) {
        this.streamName = builder.streamName;
        this.region = builder.region;
        this.partitionKey = builder.partitionKey;
    }

    public String getStreamName() {
        return streamName;
    }

    public Region getRegion() {
        return region;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String streamName;
        private Region region;
        private String partitionKey;

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        public Builder region(String regionName) {
            this.region = Region.of(regionName);
            return this;
        }

        public Builder partitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public KinesisProducerConfig build() {
            if (streamName == null || streamName.isEmpty()) {
                throw new IllegalArgumentException("Stream name is required");
            }
            if (region == null) {
                throw new IllegalArgumentException("Region is required");
            }
            return new KinesisProducerConfig(this);
        }
    }
}
