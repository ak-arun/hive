package com.ak.hive.hooks.example;

import java.io.PrintStream;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserQueryExtracterHook implements ExecuteWithHookContext {
	
	private static final Logger LOG = LoggerFactory.getLogger(UserQueryExtracterHook.class.getName());

	public void run(HookContext context) throws Exception {
		String query = context.getQueryPlan().getQueryStr();
		String userName = context.getUgi().getShortUserName();
		String log = getHookLog(userName, query);
		System.out.println("Sysout Log : "+log);
	    PrintStream stream = SessionState.getConsole().getOutStream();
	    stream.println("Console Log : "+log);
		LOG.info(log);
	}
	private String getHookLog(String user, String query){
		return "Starting UserQueryExtracterHook: User <"+user+">attempted command: <"+query+"> :Ending UserQueryExtracterHook";
	}
	
	private boolean shareResult(){
		
		try{
			
			return true;
		}catch(Exception e){
			
		}
		return false;
	}

}
