package com.ak.hive.hook.util;


public class HookConstants {

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
}
