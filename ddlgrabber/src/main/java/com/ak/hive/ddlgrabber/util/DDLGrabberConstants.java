package com.ak.hive.ddlgrabber.util;


public class DDLGrabberConstants {

	public static final String JAAS_CONFIG_WITH_KEYTAB="com.sun.security.auth.module.Krb5LoginModule required "
            + "useTicketCache=false "
            + "renewTicket=true "
            + "serviceName=\"<KAFKA_SERVICE_NAME>\" "
            + "useKeyTab=true "
            + "keyTab=\"<KAFKA_SERVICE_KEYTAB>\" "
            + "principal=\"<KAFKA_SERVICE_PRINCIPAL>\";";
	
	public static final String JAAS_CONFIG_NO_KEYTAB="com.sun.security.auth.module.Krb5LoginModule required "
            + "loginModuleName=com.sun.security.auth.module.Krb5LoginModule "
            + "renewTicket=true "
            + "serviceName=\"<KAFKA_SERVICE_NAME>\" "
            + "useKeyTab=false "
            + "storeKey=false "
            + "loginModuleControlFlag=required "
            + "useTicketCache=true;";
	
	
	public static final String DDL_HOOK_NOTIFICATION_MIN_THREAD = "ddl.hook.notification.minThread";
	public static final String DDL_HOOK_NOTIFICATION_MAX_THREAD = "ddl.hook.notification.maxThread";
	public static final String DDL_HOOK_NOTIFICATION_KEEP_ALIVE_TIME_MS = "ddl.hook.notification.keepaliveTime.ms";
	public static final int SHUTDOWN_HOOK_WAIT_TIME_MS = 3000;
	public static final String DDL_HOOK_NOTIFICATION_QUEUE_SIZE="ddl.hook.notification.queueSize";
	
	public static final String DDL_HOOK_KAFKA_USER_PRINCIPAL = "ddl.hook.kafka.userPrincipal";
	public static final String DDL_HOOK_KAFKA_USER_KEYTAB = "ddl.hook.kafka.userKeytab";
	public static final String DDL_HOOK_KAFKA_SECURITY_PROTOCOL = "ddl.hook.kafka.security.protocol";
	public static final String DDL_HOOK_KAFKA_SERVICE_NAME = "ddl.hook.kafka.serviceName";
	public static final String DDL_HOOK_KAFKA_ZK_CONNECT = "ddl.hook.kafka.zkConnect";
	public static final String DDL_HOOK_KAFKA_BOOTSTRAP_SERVERS = "ddl.hook.kafka.bootstrapServers";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String SECURITY_PROTOCOL = "security.protocol";
	public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	public static final String VALUE_SERIALIZER = "value.serializer";
	public static final String KEY_SERIALIZER = "key.serializer";
	public static final String DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_TYPE = "ddl.hook.kafka.sslcontext.truststore.type";
	public static final String DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_PASSWORD = "ddl.hook.kafka.sslcontext.truststore.password";
	public static final String DDL_HOOK_KAFKA_SSLCONTEXT_TRUSTSTORE_FILE = "ddl.hook.kafka.sslcontext.truststore.file";
	public static final String DDL_HOOK_KAFKA_TOPIC_NAME = "ddl.hook.kafka.topicName";
	public static final String HIVE_SERVER2_KERBEROS_KEYTAB = "hive.server2.authentication.kerberos.keytab";
	public static final String HIVE_SERVER2_KERBEROS_PRINCIPAL ="hive.server2.authentication.kerberos.principal";
	public static final String ALTER = "alter";
	public static final String CREATE = "create";
	public static final String DBTYPE_MYSQL = "mysql";
	public static final String DBTYPE_POSTGRES = "postgres";
	public static final String DBTYPE_HIVE = "hive";
	
	public static final String SHOW_DBS="show databases";
	public static final String SHOW_TBLS="show tables";
	public static final String SHOW_CREATE_TBL="show create table <tablename>";
	public static final String INSERT = "insert into <tablename> values (?,?,?,?)";
	public static final String USE_DBS="use <databasename>";
	
	public static final String QUIT = "QUIT";
	public static final long QUIT_TS=-1l;

	public static final String DDL_HOOK_NOTIFICATION_MAX_POOL_SIZE = "ddl.hook.nitification.max.poolSize";

	public static int SHUTDOWN_HOOK_PRIORITY=30;

	public static String DDL_HOOK_NOTIFICATION_MAX_RETRY_COUNT="ddl.hook.notification.max.retryCount";

	public static String DDL_HOOK_NOTIFICATION_RETRY_DELAY_MS = "ddl.hook.notification.retryDelay.millis";
}
