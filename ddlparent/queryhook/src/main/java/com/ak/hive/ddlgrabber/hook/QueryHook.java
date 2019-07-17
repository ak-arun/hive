package com.ak.hive.ddlgrabber.hook;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class QueryHook implements ExecuteWithHookContext {

  private static final Log LOG = LogFactory.getLog(QueryHook.class.getName());
  private static final Object LOCK = new Object();
  private static ExecutorService executor;
  private enum EntityTypes { HIVE_QUERY_ID };
  private enum EventTypes { QUERY_SUBMITTED, QUERY_COMPLETED };
  private enum OtherInfoTypes { QUERY, STATUS, TEZ, MAPRED };
  private enum PrimaryFilterTypes { user, requestuser, operationid };
  private static final int WAIT_TIME = 3;

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
            String queryId = plan.getQueryId();
            String opId = hookContext.getOperationId();
            long queryStartTime = plan.getQueryStartTime();
            String user = hookContext.getUgi().getUserName();
            String requestuser = hookContext.getUserName();
            if (hookContext.getUserName() == null ){
            	requestuser = hookContext.getUgi().getUserName() ; 
            }
            int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
            int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();
            
            
            

            switch(hookContext.getHookType()) {
            case PRE_EXEC_HOOK:
            	System.out.println("PRE_EXEC_HOOK");
              ExplainTask explain = new ExplainTask();
              explain.initialize(conf, plan, null);
              System.out.println("PRE_EXEC_HOOK INIT ExplainTask");
              String query = plan.getQueryStr();
              System.out.println("PRE_EXEC_HOOK Query "+query);
              List<Task<?>> rootTasks = plan.getRootTasks();
              System.out.println("PRE_EXEC_HOOK getRootTasks");
              JSONObject explainPlan = explain.getJSONPlan(null, null, rootTasks,
                   plan.getFetchTask(), true, false, false);
              System.out.println("PRE_EXEC_HOOK JSONObject explainPlan");
              fireAndForget(conf, createPreHookEvent(queryId, query,
                   explainPlan, queryStartTime, user, requestuser, numMrJobs, numTezJobs, opId));
              System.out.println("PRE_EXEC_HOOK fireAndForget");
              break;
            case POST_EXEC_HOOK:
              fireAndForget(conf, createPostOrFailHookEvent(queryId, currentTime, user, requestuser, true, opId));
              break;
            case ON_FAILURE_HOOK:
              fireAndForget(conf, createPostOrFailHookEvent(queryId, currentTime, user, requestuser , false, opId));
              break;
            default:
              break;
            }
          } catch (Exception e) {
            LOG.info("Failed to submit plan: " + StringUtils.stringifyException(e));
          }
        }
      });
  }

  JSONObject createPreHookEvent(String queryId, String query, JSONObject explainPlan,
      long startTime, String user, String requestuser, int numMrJobs, int numTezJobs, String opId) throws Exception {

	 System.out.println("createPreHookEvent"); 
	  
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", "pre");
    queryObj.put("queryText", query);
    queryObj.put("queryPlan", explainPlan);

    LOG.info("Received pre-hook notification for :" + queryId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Otherinfo: " + queryObj.toString());
      LOG.debug("Operation id: <" + opId + ">");
    }

    
    queryObj.put("entityId", queryId);
    queryObj.put("entityType", EntityTypes.HIVE_QUERY_ID.name());
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

  JSONObject createPostOrFailHookEvent(String queryId, long stopTime, String user, String requestuser, boolean success,
      String opId) throws JSONException {
	  System.out.println("createPostOrFailHookEvent"); 
    LOG.info("Received post-hook notification for :" + queryId);
   
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", success==true?"post":"fail");

    queryObj.put("entityId", queryId);
    queryObj.put("entityType", EntityTypes.HIVE_QUERY_ID.name());
    
    queryObj.put(PrimaryFilterTypes.user.name(), user);
    queryObj.put(PrimaryFilterTypes.requestuser.name(), requestuser);
    if (opId != null) {
    	queryObj.put(PrimaryFilterTypes.operationid.name(), opId);
    }

    queryObj.put("eventType",EventTypes.QUERY_COMPLETED.name());
    queryObj.put("eventTimestamp",stopTime);

    queryObj.put(OtherInfoTypes.STATUS.name(), success);
    return queryObj;
  }

  synchronized void fireAndForget(Configuration conf, JSONObject queryObj) throws Exception {
	  System.out.println("Notification Triggered");
	  System.out.println("Event Notification : "+ queryObj.toString());
  }
}
