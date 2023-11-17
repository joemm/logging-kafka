/******************************************************************************
 * <p>
 * Title: Override KafkaLoggingProducer.java
 * </p>
 * <p>
 * Description: Kafka Producer implementation
 * </p>
 * <p>
 * Copyright: Copyright (c) 2023 Comcast/Blueface , Dublin, Ireland.
 * </p>
 *
 * @author joe.murphy
 *
 *****************************************************************************/
package com.comast.override.logging.kafka.producer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Kafka Producer implementation<br>
 * <br>
 * key type is {@link Integer}
 *
 * @param <V> {@link GenericRecord} value type
 */
public class KafkaLoggingProducer<V> implements AutoCloseable
{

    private KafkaProducer<Integer, V> producer;
	private String topic;
	private Integer partitionKey;

	/**
	 * @param kafkaProducerSupplier
	 * @param topic
	 * @param partitionKey
	 */
	public KafkaLoggingProducer(
            final Properties props,
            final String topic,
            final Integer partitionKey,
            String serializer) {
            
            this.topic = Objects.requireNonNull(topic, "'topic' arg must not be null");
            this.partitionKey = partitionKey;
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
            this.producer = new KafkaProducer<>(props);
	}

	/**
	 * @param message
	 * @return
	 * @throws IllegalStateException if called after {@link #close()}
	 */
	public Future<RecordMetadata> send(final V message) {
		return send(null, message);
	}

	/**
	 * @param key
	 * @param message
	 * @return
	 * @throws IllegalStateException if called after {@link #close()}
	 */
	public Future<RecordMetadata> send(final Integer key, final V message) {

		final ProducerRecord<Integer, V> record = new ProducerRecord<>(
				this.topic, this.partitionKey, key, message);
		return this.producer.send(record);
	}

	@Override
	public void close() {
		producer.close();
	}
}
