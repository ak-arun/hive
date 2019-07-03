package com.ak.hive.hooks.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

public class TestKafka2 {
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		Map<String, Object> mapToPopulate = new HashMap<String, Object>();
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		
        
        mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.getProperty("truststore.file"));
        mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getProperty("truststore.password"));
        mapToPopulate.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getProperty("truststore.type"));
		
        
        mapToPopulate.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required "
                + "useTicketCache=false "
                + "renewTicket=true "
                + "serviceName=\"" + properties.getProperty("service.name") + "\" "
                + "useKeyTab=true "
                + "keyTab=\"" + properties.getProperty("service.keytab")  + "\" "
                + "principal=\"" +  properties.getProperty("service.principal")  + "\";");
        
        for(Object key : properties.keySet()){
        	mapToPopulate.put(key.toString(), properties.getProperty(key.toString()));
        }
        
       KafkaProducer<String, String> producer = new KafkaProducer<String,String>(mapToPopulate);
       ProducerRecord<String, String> record = new ProducerRecord<String, String>(properties.getProperty("topic.name"), "Hello World"+System.currentTimeMillis());
		
	producer.send(record, new Callback() {
		
		@Override
		public void onCompletion(RecordMetadata recordSent, Exception e) {
			if (e==null){
				System.out.println("sent successfully");
			}else{
				e.printStackTrace();
			}
			
		}
	} );
	
	producer.close();
		
	}

}
