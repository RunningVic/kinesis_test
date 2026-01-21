/**
 * Kinesis Producer Utility Package
 * 
 * <p>This package provides utilities for producing records to Amazon Kinesis Data Streams.
 * It supports both String and AVRO data formats, with synchronous and asynchronous operations.
 * 
 * <p>Example usage:
 * <pre>
 * // String Producer
 * StringKinesisProducer producer = KinesisProducerFactory.createStringProducer(Region.US_EAST_1);
 * producer.sendString("my-stream", "key", "data");
 * 
 * // AVRO Producer
 * AvroKinesisProducer avroProducer = KinesisProducerFactory.createAvroProducer(Region.US_EAST_1);
 * avroProducer.sendAvro("my-stream", "key", avroRecord);
 * </pre>
 * 
 * @since 1.0.0
 */
package com.example.kinesis.producer;
