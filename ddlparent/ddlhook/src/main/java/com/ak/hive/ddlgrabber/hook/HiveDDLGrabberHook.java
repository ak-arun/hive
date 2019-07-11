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
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.ak.hive.ddlgrabber.util.DDLHookNotificationHandler;


public class HiveDDLGrabberHook implements ExecuteWithHookContext {


	private String query;
	private HiveConf configuration;
	private Map<String, Object> propertyMap;
	private JSONObject notificationObject;
	private ExecutorService executorService = null;
	private Table table=null;

	
	private static final Logger LOG = LoggerFactory.getLogger(HiveDDLGrabberHook.class);
	 
	// TODO adapt to all kinds of install. Current support for SASL_SSL+ ssl trustore configs
	

	@SuppressWarnings("unchecked")
	public void run(HookContext hookContext) throws Exception {

			query = hookContext.getQueryPlan().getQueryStr();
			
			configuration = hookContext.getConf();

			propertyMap = new HashMap<String, Object>();
			
			// these configs are to be read from the hive conf, set them in hive-site or per session
			
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD));
			propertyMap.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE,"JKS"));
			propertyMap.put(DDLGrabberConstants.KEY_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.VALUE_SERIALIZER,DDLGrabberConstants.STRING_SERIALIZER);
			propertyMap.put(DDLGrabberConstants.BOOTSTRAP_SERVERS, configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_BOOTSTRAP_SERVERS));
			propertyMap.put(DDLGrabberConstants.ZOOKEEPER_CONNECT,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_ZK_CONNECT));
			propertyMap.put(DDLGrabberConstants.SASL_KERBEROS_SERVICE_NAME,configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME));
			propertyMap.put(DDLGrabberConstants.SECURITY_PROTOCOL, configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SECURITY_PROTOCOL));
			
			// it is going to be 1 message per run (if ddl) . Need retry ? Or just log if message could not be delivered 
			
		/*	int retryCount = configuration.getInt(DDLGrabberConstants.DDL_HOOK_NOTIFICATION_MAX_RETRY_COUNT, 0);
			propertyMap.put("retries", retryCount);
			if(retryCount>0){
				propertyMap.put("enable.idempotence", true);
			}*/
			
			executorService = Executors.newFixedThreadPool(1);
			
			/*
			 * Dynamic JAAS Configuration as in
			 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients
			 */
			
			
			if(isDDL()){
				
			// if is create or alter
				
			final ProducerRecord<String, String> record = generateNotificationRecord(hookContext);
			// json message 
			
			if(UserGroupInformation.isLoginKeytabBased()){
				// as in hs2, use the hive service principal for authorizing kafka writes
				
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
				
			}
			else{
				// as in hive cli, use logged in user name for authorizing kafka writes
				propertyMap.put(SaslConfigs.SASL_JAAS_CONFIG,DDLGrabberConstants.JAAS_CONFIG_NO_KEYTAB
						.replace(
								"<KAFKA_SERVICE_NAME>",configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_SERVICE_NAME)));
				hookContext.getUgi().doAs(new PrivilegedExceptionActionImplementation(record, executorService, propertyMap));
			}
			executorService.shutdown();
			}
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
		private final ExecutorService executorService;
		private final Map<String, Object> propertyMap;

		private PrivilegedExceptionActionImplementation(
				ProducerRecord<String, String> record,ExecutorService executorService,Map<String, Object> propertyMap) {
			this.record = record;
			this.executorService=executorService;
			this.propertyMap=propertyMap;
		}

		public Object run() throws Exception {
			executorService.submit(new Callable<Object>() {
				public Object call() throws Exception {
					new DDLHookNotificationHandler(propertyMap, record).send();
					return null;
				}
			});
			return null;
		}
	}
	private ProducerRecord<String, String> generateNotificationRecord(HookContext hookContext) throws JSONException{
		notificationObject = new JSONObject();
		for(WriteEntity output : hookContext.getOutputs()){
			//databaseName = databaseName==null?(output.getDatabase()!=null?output.getDatabase().getName():null):databaseName;
			//tableName = tableName==null?(output.getTable()!=null?output.getTable().getTableName():null):tableName;
			table = table==null?(output.getTable()!=null?output.getTable():null):table;
		}
		notificationObject.put("db_name", table.getDbName());
		notificationObject.put("table_name", table.getTableName());
		notificationObject.put("dump_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
		notificationObject.put("ddl", query);
		return new ProducerRecord<String, String>(configuration.get(DDLGrabberConstants.DDL_HOOK_KAFKA_TOPIC_NAME), notificationObject.toString());
	}
}
