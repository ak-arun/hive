package com.ak.hive.ddlgrabber.util;

import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DDLHookNotificationHandler {

	public DDLHookNotificationHandler(Map<String, Object> kafkaProperties,
			ProducerRecord<String, String> record) {
		this.kafkaProperties = kafkaProperties;
		this.record = record;
	}

	private static final Logger LOG = LoggerFactory
			.getLogger(DDLHookNotificationHandler.class);

	private Map<String, Object> kafkaProperties;
	private KafkaProducer<String, String> producer;
	ProducerRecord<String, String> record;

	public void send() {
		producer = new KafkaProducer<String, String>(kafkaProperties);
		producer.send(record, new Callback() {
			public void onCompletion(RecordMetadata metadata,
					Exception exception) {
				if (exception != null) {
					exception.printStackTrace();
					LOG.info("Exception while Publishing to kafka"
							+ DDLGrabberUtils.getTraceString(exception));
				}
			}
		});
		producer.close();
	}

}
