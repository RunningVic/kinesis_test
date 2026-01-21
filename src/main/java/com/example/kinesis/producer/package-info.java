/**
 * Kinesis Producer Utility Package
 * 
 * <p>This package provides a simple utility for producing String messages to Amazon Kinesis Data Streams.
 * 
 * <p>Example usage:
 * <pre>
 * KinesisProducerConfig config = new KinesisProducerConfig().setRegion(Region.US_EAST_1);
 * StringKinesisProducer producer = new StringKinesisProducer(config);
 * producer.send("my-stream", "key", "message");
 * producer.close();
 * </pre>
 * 
 * @since 1.0.0
 */
package com.example.kinesis.producer;
