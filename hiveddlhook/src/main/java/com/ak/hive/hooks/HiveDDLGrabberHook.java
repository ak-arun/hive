package com.ak.hive.hooks;


import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import com.ak.hive.ddl.hook.util.HookConstants;

public class HiveDDLGrabberHook implements ExecuteWithHookContext {


	private String query;
	private HiveConf configuration;
	private Map<String, Object> propertyMap;
	private KafkaProducer<String, String> producer;

	
	 //private static final Logger LOG = LoggerFactory.getLogger(HiveDDLGrabberHook.class);
	 
	// TODO adapt to all kinds of install. Currently coding for SASL_SSL+ ssl trustore only config

	@SuppressWarnings("unchecked")
	public void run(HookContext hookContext) throws Exception {

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
			
			
			if(isDDL()){
			
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>(configuration.get(HookConstants.DDL_HOOK_KAFKA_TOPIC_NAME), query);
			
			if(UserGroupInformation.isLoginKeytabBased()){
				
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,HookConstants.JAAS_CONFIG_WITH_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(HookConstants.DDL_HOOK_KAFKA_SERVICE_NAME))
						.replace(
								"<KAFKA_SERVICE_KEYTAB>",configuration.get(HookConstants.HIVE_SERVER2_KERBEROS_KEYTAB))
						.replace(
								"<KAFKA_SERVICE_PRINCIPAL>",configuration.get(HookConstants.HIVE_SERVER2_KERBEROS_PRINCIPAL).replace("_HOST", InetAddress.getLocalHost().getCanonicalHostName())));
				send(record);
			}
			else{
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,HookConstants.JAAS_CONFIG_NO_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(HookConstants.DDL_HOOK_KAFKA_SERVICE_NAME)));
				hookContext.getUgi().doAs(new PrivilegedExceptionActionImplementation(record));
			}
			}
	}
	
	private void send(ProducerRecord<String, String> record){
		producer = new KafkaProducer<String,String>(propertyMap);
		producer.send(record , new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception!=null){
					exception.printStackTrace();
				}
			}
		});
		producer.close();
	}
	private boolean isDDL() {
		try{
			return query.toLowerCase().trim().startsWith(HookConstants.CREATE)||query.toLowerCase().trim().startsWith(HookConstants.ALTER) ? true : false;
		}catch(Exception e){}
		return false;
	}
	
	
	@SuppressWarnings("rawtypes")
	private final class PrivilegedExceptionActionImplementation implements
			PrivilegedExceptionAction {
		private final ProducerRecord<String, String> record;

		private PrivilegedExceptionActionImplementation(
				ProducerRecord<String, String> record) {
			this.record = record;
		}

		public Object run() throws Exception {
			send(record);
			return null;
		}
	}
}
