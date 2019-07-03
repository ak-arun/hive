package com.ak.hive.hooks.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import com.ak.hive.hook.util.HookConstants;

public class HiveDDLGrabberHook implements ExecuteWithHookContext {

	private String query;
	private HiveConf configuration;
	private Map<String, Object> propertyMap;

	// TODO adapt to all kinds of install. Currently coding for SASL_SSL+ ssl trustore only config

	@Override
	public void run(HookContext hookContext) throws Exception {

		if (hookContext.getHookType() == HookType.POST_EXEC_HOOK) {
			query = hookContext.getQueryPlan().getQueryStr();

			configuration = hookContext.getConf();


			propertyMap = new HashMap<String, Object>();

			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,configuration.get(HookConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,configuration.get(HookConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,configuration.get(HookConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE,"JKS"));

			propertyMap.put(HookConstants.KEY_SERIALIZER,HookConstants.STRING_SERIALIZER);
			propertyMap.put(HookConstants.VALUE_SERIALIZER,HookConstants.STRING_SERIALIZER);
			propertyMap.put(HookConstants.BOOTSTRAP_SERVERS, configuration.get(HookConstants.DDL_HOOK_KAFKA_BOOTSTRAP_SERVERS));
			propertyMap.put(HookConstants.ZOOKEEPER_CONNECT,configuration.get(HookConstants.DDL_HOOK_KAFKA_ZK_CONNECT));
			propertyMap.put(HookConstants.SASL_KERBEROS_SERVICE_NAME,configuration.get(HookConstants.DDL_HOOK_KAFKA_SERVICE_NAME));
			propertyMap.put(HookConstants.SECURITY_PROTOCOL, configuration.get(HookConstants.DDL_HOOK_KAFKA_SECURITY_PROTOCOL));

			/*
			 * Dynamic JAAS Configuration as in
			 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients
			 */
			propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,HookConstants.JAAS_CONFIG
									.replace(
											"<KAFKA_SERVICE_NAME>",configuration.get(HookConstants.DDL_HOOK_KAFKA_SERVICE_NAME))
									.replace(
											"<KAFKA_SERVICE_KEYTAB>",configuration.get(HookConstants.DDL_HOOK_KAFKA_USER_KEYTAB))
									.replace(
											"<KAFKA_SERVICE_PRINCIPAL>",configuration.get(HookConstants.DDL_HOOK_KAFKA_USER_PRINCIPAL)));
			
		  // TODO move to an async executor
			
			if(isDDL()){
				KafkaProducer<String, String> producer = new KafkaProducer<String,String>(propertyMap);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(configuration.get(HookConstants.DDL_HOOK_KAFKA_TOPIC_NAME), query);
				producer.send(record , new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null){
							exception.printStackTrace();
						}
						
					}
				});
				producer.close();
			}

		}

	}

	private boolean isDDL() {
		
		try{
			
			return query.toLowerCase().trim().startsWith("create")||query.toLowerCase().trim().startsWith("alter") ? true : false;
			
		}catch(Exception e){}
		return false;
	}

}
