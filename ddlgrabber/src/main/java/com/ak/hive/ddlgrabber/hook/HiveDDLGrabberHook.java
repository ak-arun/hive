package com.ak.hive.ddlgrabber.hook;


import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;


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
			
			for(ReadEntity input : hookContext.getInputs()){
				System.out.println("INPUT DB Name "+input.getDatabase());
				System.out.println("INPUT Table Name "+input.getTable().getCompleteName());
			}
			
			for(WriteEntity output :hookContext.getOutputs()){
				System.out.println("OUTPUT DB Name "+output.getDatabase());
				System.out.println("OUTPUT Table Name "+output.getTable().getCompleteName());
			}
			
			
			
			configuration = hookContext.getConf();

			propertyMap = new HashMap<String, Object>();
			
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE,"JKS"));
			propertyMap.put(DDLGrabberConstants.KEY_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.VALUE_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.BOOTSTRAP_SERVERS, configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_BOOTSTRAP_SERVERS));
			propertyMap.put(DDLGrabberConstants.ZOOKEEPER_CONNECT,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_ZK_CONNECT));
			propertyMap.put(DDLGrabberConstants.SASL_KERBEROS_SERVICE_NAME,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME));
			propertyMap.put(DDLGrabberConstants.SECURITY_PROTOCOL, configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SECURITY_PROTOCOL));
			
			
			/*
			 * Dynamic JAAS Configuration as in
			 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients
			 */
			
			
			if(isDDL()){
			
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>(configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_TOPIC_NAME), query);
			
			if(UserGroupInformation.isLoginKeytabBased()){
				
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_WITH_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME))
						.replace(
								"<KAFKA_SERVICE_KEYTAB>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_KEYTAB))
						.replace(
								"<KAFKA_SERVICE_PRINCIPAL>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_PRINCIPAL).replace("_HOST", InetAddress.getLocalHost().getCanonicalHostName())));
				send(record);
			}
			else{
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_NO_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME)));
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
			return query.toLowerCase().trim().startsWith(DDLGrabberConstants.CREATE)||query.toLowerCase().trim().startsWith(DDLGrabberConstants.ALTER) ? true : false;
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
