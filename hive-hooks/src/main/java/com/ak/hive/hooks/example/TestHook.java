package com.ak.hive.hooks.example;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Properties;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import com.ak.hive.hook.example.conf.InMemoryJAASConfiguration;

public class TestHook implements ExecuteWithHookContext{

	public void run(HookContext context) throws Exception {
		
		// check if hive conf reflects under hook
		
		HiveConf conf = context.getConf();
		String userName = context.getUgi().getShortUserName();
		String property = conf.get("test.hook.conf");
		context.getUgi();
		System.out.println("Sysout Log : "+property+" user "+userName+" isLoginKeytabBased() "+UserGroupInformation.isLoginKeytabBased());
	    PrintStream stream = SessionState.getConsole().getOutStream();
	    stream.println("Console Log : "+property+" user "+userName);
	    
	    // additions
	    //conf.get(arg0)
	    
	    Properties krbProperties = new Properties();
	    krbProperties.setProperty("ddl.hook.KafkaClient.option.keyTab","/etc/security/keytabs/hive.service.keytab");
	    krbProperties.setProperty("ddl.hook.KafkaClient.option.principal","hive/"+InetAddress.getLocalHost().getCanonicalHostName()+"@"+conf.get("ddl.hook.krb.realm"));
	    krbProperties.setProperty("ddl.hook.ticketBased-KafkaClient.loginModuleControlFlag","required");
	    krbProperties.setProperty("ddl.hook.ticketBased-KafkaClient.loginModuleName","com.sun.security.auth.module.Krb5LoginModule");
	    krbProperties.setProperty("ddl.hook.ticketBased-KafkaClient.option.useTicketCache","true");
	    InMemoryJAASConfiguration.init(krbProperties);
	    
	    /*
	     * KafkaClient.option.keyTab=/etc/security/keytabs/hive.service.keytab
atlas.jaas.KafkaClient.option.principal=hive/_HOST@TECH.HDP.NEWYORKLIFE.COM
atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag=required
atlas.jaas.ticketBased-KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule
atlas.jaas.ticketBased-KafkaClient.option.useTicketCache=true
	     * 
	     * */
		
	}

}
