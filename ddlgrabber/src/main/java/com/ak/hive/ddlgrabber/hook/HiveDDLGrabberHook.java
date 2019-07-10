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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.ak.hive.ddlgrabber.util.DDLHookNotificationHandler;
import com.ak.hive.ddlgrabber.util.Utils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class HiveDDLGrabberHook implements ExecuteWithHookContext {


	private String query;
	private String tableName;
	private String databaseName;
	private HiveConf configuration;
	private Map<String, Object> propertyMap;
	//private KafkaProducer<String, String> producer;
	private String notification;
	//private ExecutorService executor = null;
	private ExecutorService executorService = null;

	
	private static final Logger LOG = LoggerFactory.getLogger(HiveDDLGrabberHook.class);
	 
	// TODO adapt to all kinds of install. Currently coding for SASL_SSL+ ssl trustore only config

	@SuppressWarnings("unchecked")
	public void run(HookContext hookContext) throws Exception {

			query = hookContext.getQueryPlan().getQueryStr();
			
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
			
			//int maxNotifThreads = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_MAX_POOL_SIZE, 1);
			
			
			executorService = Executors.newFixedThreadPool(1);
			
			/*
			int minNotifThreads = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_MIN_THREAD, 1);
			int maxNotifThreads = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_MAX_THREAD, 1);
			long keepAliveTimeMs = configuration.getLong(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_KEEP_ALIVE_TIME_MS, 10000);
			int  queueSize       = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_QUEUE_SIZE, 10000);
			
			*/
			
			/*
			 * Dynamic JAAS Configuration as in
			 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients
			 */
			
			
			if(isDDL()){
				
				/*executor = new ThreadPoolExecutor(minNotifThreads, maxNotifThreads, keepAliveTimeMs, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(queueSize), new ThreadFactoryBuilder().setNameFormat("DDL Hook Notifier %d ").setDaemon(true).build());
				
				ShutdownHookManager.get().addShutdownHook(new Thread() {
					@Override
					public void run() {
						try{
							executor.shutdown();
			                executor.awaitTermination(DDLGrabberConstants.SHUTDOWN_HOOK_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
			                executor = null;
			            }
						catch(Exception e){
							LOG.info("Exception in shutdown "+Utils.getTraceString(e));
							} 
						finally {
							LOG.info("Executor Shutdown");
						}
					}
					
				}, DDLGrabberConstants.SHUTDOWN_HOOK_PRIORITY);
				*/
				
			/*	for(WriteEntity output : hookContext.getOutputs()){
					databaseName = databaseName==null?(output.getDatabase()!=null?output.getDatabase().getName():null):databaseName;
					tableName = tableName==null?(output.getTable()!=null?output.getTable().getTableName():null):tableName;
				}*/
			
			final ProducerRecord<String, String> record = generateNotificationRecord(hookContext);
			
			if(UserGroupInformation.isLoginKeytabBased()){
				
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_WITH_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME))
						.replace(
								"<KAFKA_SERVICE_KEYTAB>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_KEYTAB))
						.replace(
								"<KAFKA_SERVICE_PRINCIPAL>",configuration.get(DDLGrabberConstants.HIVE_SERVER2_KERBEROS_PRINCIPAL).replace("_HOST", InetAddress.getLocalHost().getCanonicalHostName())));
				
				executorService.submit(new Callable<Object>() {
					public Object call() throws Exception {
						new DDLHookNotificationHandler(propertyMap, record).send();
						return null;
					}
				});
				
				//send(record);
				
			}
			else{
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_NO_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME)));
				hookContext.getUgi().doAs(new PrivilegedExceptionActionImplementation(record, executorService, propertyMap));
			}
			executorService.shutdown();
			}
	}
	
	/*private void send(ProducerRecord<String, String> record){
		producer = new KafkaProducer<String,String>(propertyMap);
		producer.send(record , new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception!=null){
					exception.printStackTrace();
				}
			}
		});
		producer.close();
	}*/
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
		private final ExecutorService executorService;
		private final Map<String, Object> propertyMap;

		private PrivilegedExceptionActionImplementation(
				ProducerRecord<String, String> record,ExecutorService executorService,Map<String, Object> propertyMap) {
			this.record = record;
			this.executorService=executorService;
			this.propertyMap=propertyMap;
		}

		public Object run() throws Exception {
			//send(record);
			executorService.submit(new Callable<Object>() {
				public Object call() throws Exception {
					new DDLHookNotificationHandler(propertyMap, record).send();
					return null;
				}
			});
			return null;
		}
	}
	private ProducerRecord<String, String> generateNotificationRecord(HookContext hookContext){
		for(WriteEntity output : hookContext.getOutputs()){
			databaseName = databaseName==null?(output.getDatabase()!=null?output.getDatabase().getName():null):databaseName;
			tableName = tableName==null?(output.getTable()!=null?output.getTable().getTableName():null):tableName;
		}
		notification = "_database="+databaseName+"_table="+tableName+"_datetime="+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())+"_query="+query;
		return new ProducerRecord<String, String>(configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_TOPIC_NAME), notification);
	}
}
