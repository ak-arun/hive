package com.ak.hive.hooks.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestKafka {
public static void main(String[] args) throws FileNotFoundException, IOException {
	
	Properties properties = new Properties();
	properties.load(new FileReader(new File(args[0])));
	
	properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put("max.poll.records", 1);
    properties.put("enable.auto.commit", false);
    properties.put("auto.commit.enable", false);
    properties.put("session.timeout.ms", 30000);
    properties.put("zookeeper.connection.timeout.ms", 30000);
    properties.put("zookeeper.session.timeout.ms",60000);
    properties.put("zookeeper.sync.time.ms",20);
    
    KafkaProducer producer = new KafkaProducer(properties);
    ProducerRecord record = new ProducerRecord<String, String>(properties.getProperty("topic.name"), "Test Message "+System.currentTimeMillis());
	producer.send(record);
	System.out.println("Done");
}
}
