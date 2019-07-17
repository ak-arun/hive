package com.ak.hive.ddlgrabber.hook;


import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.ak.hive.ddlgrabber.util.DDLGrabberUtils;


public class QueryGrabberHook implements ExecuteWithHookContext {


	private static final String HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE = "hivehook.kafka.sslcontext.truststore.type";
	private static final String HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD = "hivehook.kafka.sslcontext.truststore.password";
	private static final String HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE = "hivehook.kafka.sslcontext.truststore.file";
	private static final String HIVEHOOK_KAFKA_TOPIC_NAME = "hivehook.kafka.topicName";
	private static final String HIVEHOOK_KAFKA_SECURITY_PROTOCOL = "hivehook.kafka.security.protocol";
	private static final String HIVEHOOK_KAFKA_SERVICE_NAME = "hivehook.kafka.serviceName";
	private static final String HIVEHOOK_KAFKA_BOOTSTRAP_SERVERS = "hivehook.kafka.bootstrapServers";
	
	private String queryString;
	private HiveConf configuration;
	private Map<String, Object> propertyMap;
	private JSONObject notificationObject;
	private ExecutorService executorService = null;
	private Table table=null;
	
	
	private HookContext context;

	
	private static final Logger LOG = LoggerFactory.getLogger(QueryGrabberHook.class);
	 
	// TODO adapt to all kinds of install. Current support for SASL_SSL+ ssl trustore configs
	

	@SuppressWarnings("unchecked")
	public void run(HookContext hookContext) throws Exception {
		
			context = hookContext;

			queryString = hookContext.getQueryPlan().getQueryStr();
			
			configuration = hookContext.getConf();

			propertyMap = new HashMap<String, Object>();
			
			// these configs are to be read from the hive conf, set them in hive-site or per session
			
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,configuration.get(HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,configuration.get(HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,configuration.get(HIVEHOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE,"JKS"));
			propertyMap.put(DDLGrabberConstants.KEY_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.VALUE_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.BOOTSTRAP_SERVERS, configuration.get(HIVEHOOK_KAFKA_BOOTSTRAP_SERVERS));
			propertyMap.put(DDLGrabberConstants.SASL_KERBEROS_SERVICE_NAME,configuration.get(HIVEHOOK_KAFKA_SERVICE_NAME));
			propertyMap.put(DDLGrabberConstants.SECURITY_PROTOCOL, configuration.get(HIVEHOOK_KAFKA_SECURITY_PROTOCOL));
			
			// it is going to be 1 message per run (if ddl) . Need retry ? Or just log if message could not be delivered 
			
		/*	int retryCount = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_MAX_RETRY_COUNT, 0);
			propertyMap.put("retries", retryCount);
			if(retryCount>0){
				propertyMap.put("enable.idempotence", true);
			}*/
			
			executorService = Executors.newFixedThreadPool(1);
			
			
			if(isDDL()){
				
			// if is create or alter
				
			final ProducerRecord<String, String> record = generateNotificationRecord(hookContext);
			// json message 
			
			
			/*
			 * Dynamic JAAS Configuration as in
			 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients
			 */
			
			if(UserGroupInformation.isLoginKeytabBased()){
				// as in hs2, use the hive service principal for authorizing kafka writes
				
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_WITH_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(HIVEHOOK_KAFKA_SERVICE_NAME))
						.replace(
								"<KAFKA_SERVICE_KEYTAB>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_KEYTAB))
						.replace(
								"<KAFKA_SERVICE_PRINCIPAL>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_PRINCIPAL).replace("_HOST", InetAddress.getLocalHost().getCanonicalHostName())));
				
				executorService.submit(new Callable<Object>() {
					public Object call() throws Exception {
						notifyRecord(record);
						return null;
					}
				});
				
			}
			else{
				// as in hive cli, use logged in user name for authorizing kafka writes
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_NO_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(HIVEHOOK_KAFKA_SERVICE_NAME)));
				hookContext.getUgi().doAs(new PrivilegedExceptionActionImplementation(record, executorService));
			}
			executorService.shutdown();
			}
	}
	
	private boolean isDDL() {
		try{
			return queryString.toLowerCase().trim().startsWith(DDLGrabberConstants.CREATE)||queryString.toLowerCase().trim().startsWith(DDLGrabberConstants.ALTER) ? true : false;
		}catch(Exception e){}
		return false;
	}
	
	
	@SuppressWarnings("rawtypes")
	private final class PrivilegedExceptionActionImplementation implements
			PrivilegedExceptionAction {
		private final ProducerRecord<String, String> record;
		private final ExecutorService executorService;

		private PrivilegedExceptionActionImplementation(
				ProducerRecord<String, String> record,ExecutorService executorService) {
			this.record = record;
			this.executorService=executorService;
		}

		public Object run() throws Exception {
			executorService.submit(new Callable<Object>() {
				public Object call() throws Exception {
					notifyRecord(record);
					return null;
				}
			});
			return null;
		}
	}
	private ProducerRecord<String, String> generateNotificationRecord(HookContext hookContext) throws JSONException{
		
		 long currentTime = System.currentTimeMillis();
	     HiveConf conf = new HiveConf(hookContext.getConf());
	     QueryPlan plan = hookContext.getQueryPlan();
	     Query query = plan.getQuery();
		
	     
	     
	     notificationObject = new JSONObject();
		
		
		
		String queryString1 = plan.getQueryString();
		UserGroupInformation ugi = hookContext.getUgi();
		
		
		String queryId = query.getQueryId();
		//queryFromPlan
		
		String userName = hookContext.getUserName()==null?ugi.getUserName():hookContext.getUserName();
		
		
		notificationObject.put("dump_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
		notificationObject.put("query", queryString);
		
		
		
		
		
		if (queryString.toLowerCase().trim().startsWith("create database")
				|| (queryString.toLowerCase().trim().startsWith("alter database"))
				|| (queryString.toLowerCase().trim().startsWith("alter schema"))
				|| (queryString.toLowerCase().trim().startsWith("create schema"))) {
			String dbName = null;
			for (WriteEntity output : hookContext.getOutputs()) {
				dbName = dbName == null ? (output.getDatabase() != null ? output
						.getDatabase().getName() : null)
						: dbName;
			}
			notificationObject.put("db_name", dbName);
			notificationObject.put("table_name", "");
		}else{
			for(WriteEntity output : hookContext.getOutputs()){
				table = table==null?(output.getTable()!=null?output.getTable():null):table;
			}
			notificationObject.put("db_name", table.getDbName());
			notificationObject.put("table_name", table.getTableName());
		}
		return new ProducerRecord<String, String>(configuration.get(HIVEHOOK_KAFKA_TOPIC_NAME), notificationObject.toString());
	}
	
	public void notifyRecord(ProducerRecord<String, String> record) {
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propertyMap);
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
