package com.ak.hive.hooks.example;

import java.io.PrintStream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

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
		
	}

}
