package com.ak.hive.ddlgrabber.hook;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.ak.hive.ddlgrabber.util.DDLGrabberUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class QueryHook implements ExecuteWithHookContext {

  private static final Log LOG = LogFactory.getLog(QueryHook.class.getName());
  private static final Object LOCK = new Object();
  private static ExecutorService executor;
  private enum EventTypes { QUERY_SUBMITTED, QUERY_COMPLETED };
  private enum OtherInfoTypes { QUERY, STATUS, TEZ, MAPRED };
  private enum PrimaryFilterTypes { user, requestuser, operationid };
  private static final int WAIT_TIME = 3;
  private static final List<String> DDL_START_WORDS = Arrays.asList(new String [] {"CREATE","ALTER","DROP"});
  private static final List<String>DB_START_WORDS= Arrays.asList(new String [] {"DATABASE","SCHEMA"});

  public QueryHook() {
    synchronized(LOCK) {
      if (executor == null) {
        executor = Executors.newSingleThreadExecutor(
           new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryHook Logger %d").build());
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              executor.shutdown();
              executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
              executor = null;
            } catch(InterruptedException ie) { }
          }
        });
      }
    }

    LOG.info("Created Query Hook");
  }

  @Override
  public void run(final HookContext hookContext) throws Exception {
    final long currentTime = System.currentTimeMillis();
    final HiveConf conf = new HiveConf(hookContext.getConf());

    executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            QueryPlan plan = hookContext.getQueryPlan();
            if (plan == null) {
              return;
            }
            String opId = hookContext.getOperationId();
            long queryStartTime = plan.getQueryStartTime();
            String user = hookContext.getUgi().getUserName();
            String requestuser = hookContext.getUserName() == null ? user : hookContext.getUserName();
            int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
            int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();
            String queryId = plan.getQueryId();
            
            String queryString = plan.getQueryStr();   

            switch(hookContext.getHookType()) {
            case PRE_EXEC_HOOK:
              fireAndForget(generatePreExecutionNotification(queryId,
                   queryStartTime, user, requestuser, numMrJobs, numTezJobs, opId));
              break;
            case POST_EXEC_HOOK:
              fireAndForget(generatePostExecutionNotification(queryId, currentTime, user, requestuser, true, opId,queryString,hookContext.getOutputs(), hookContext.getInputs()));
              break;
            case ON_FAILURE_HOOK:
              fireAndForget(generatePostExecutionNotification(queryId, currentTime, user, requestuser , false, opId, queryString,hookContext.getOutputs(), hookContext.getInputs()));
              break;
            default:
              break;
            }
          } catch (Exception e) {
            LOG.info("Failed to submit plan: "+ StringUtils.stringifyException(e));
          }
        }
      });
  }

  JSONObject generatePreExecutionNotification(String queryId,
      long startTime, String user, String requestuser, int numMrJobs, int numTezJobs, String opId) throws Exception {

	  
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", "pre");
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received pre-hook notification for :" + queryId);
      LOG.debug("Otherinfo: " + queryObj.toString());
      LOG.debug("Operation id: <" + opId + ">");
    }

    
    queryObj.put("queryId", queryId);
    queryObj.put(PrimaryFilterTypes.user.name(), user);
    queryObj.put(PrimaryFilterTypes.requestuser.name(), requestuser);
    
    if (opId != null) {
    	queryObj.put(PrimaryFilterTypes.operationid.name(), opId);
    }
    queryObj.put("eventType", EventTypes.QUERY_SUBMITTED.name());
    queryObj.put("eventTimestamp", startTime);
    queryObj.put(OtherInfoTypes.TEZ.name(), numTezJobs > 0);
    queryObj.put(OtherInfoTypes.MAPRED.name(), numMrJobs > 0);
    
   
    
    return queryObj;
  }

  JSONObject generatePostExecutionNotification(String queryId, long stopTime, String user, String requestuser, boolean success,
      String opId, String queryString, Set<WriteEntity> outputs, Set<ReadEntity> inputs) throws JSONException {
    LOG.info("Received post-hook notification for :" + queryId);
   
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", success==true?"post":"fail");
    queryObj.put("queryId", queryId);
    queryObj.put("queryText", queryString);
    queryObj.put(PrimaryFilterTypes.user.name(), user);
    queryObj.put(PrimaryFilterTypes.requestuser.name(), requestuser);
    if (opId != null) {
    	queryObj.put(PrimaryFilterTypes.operationid.name(), opId);
    }
    queryObj.put("eventType",EventTypes.QUERY_COMPLETED.name());
    queryObj.put("eventTimestamp",stopTime);
    queryObj.put(OtherInfoTypes.STATUS.name(), success);
    
    if(success&&isDDL(queryString)){
    	String dbName=null;
    	String tableName=null;
    	queryObj.put("isDDL", true);
    	queryObj.put("dump_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
    	if(isDBCommand(queryString)){
    		for(WriteEntity output:outputs){
    			dbName=dbName==null?(output.getDatabase() == null ? null : output.getDatabase().getName()):dbName;
    		}
    		tableName="";
    	}
    	
    	if(dbName==null){
    		Table table=null;
    		for(WriteEntity output:outputs){
    		table = table==null?(output.getTable()!=null?output.getTable():null):table;
    		}
    		try{
    			tableName = table.getTableName();
    			dbName = table.getDbName();
    		}catch(Exception e){
    			tableName = tableName!=null?tableName:"";
    			dbName=dbName!=null?dbName:"";
    			LOG.info("Error processing query "+queryId+" exception trace "+DDLGrabberUtils.getTraceString(e));
    		}
    	}
    	queryObj.put("db_name", dbName);
    	queryObj.put("table_name", tableName);
    }
    return queryObj;
  }

  synchronized void fireAndForget(JSONObject queryObj) throws Exception {
	  System.out.println("Event Notification : "+ queryObj.toString());
  }
  
  private boolean isDDL(String query) {
		try{
			return DDL_START_WORDS.contains((query.trim().toUpperCase().split(" "))[0]);
		}catch(Exception e){}
		return false;
	}
  
  private boolean isDBCommand(String query) {
		try{
			return DB_START_WORDS.contains(((query.trim().toUpperCase().split(" "))[1]).trim());
		}catch(Exception e){}
		return false;
	}
}
