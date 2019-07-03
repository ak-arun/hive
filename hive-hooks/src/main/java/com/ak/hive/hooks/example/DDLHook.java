package com.ak.hive.hooks.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DDLHook implements ExecuteWithHookContext {
	
	 private       KafkaProducer producer;
	
	private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<String, HiveOperation>();
	
	static{
		for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }
	}

	public void run(HookContext hookContext) throws Exception {
		
		HiveOperation  oper    = OPERATION_MAP.get(hookContext.getOperationName());
		switch (oper) {
		
		/*case ALTERTABLE_ADDCOLS : break;
		case ALTERTABLE_ADDPARTS:break;
		case ALTERTABLE_ARCHIVE : break;
		case ALTERTABLE_BUCKETNUM:break;
		case ALTERTABLE_CLUSTER_SORT:break;
		case ALTERTABLE_COMPACT:break;
		case ALTERT*/
		case CREATETABLE :
			
			UserGroupInformation ugi = hookContext.getUgi() == null ? Utils.getUGI() : hookContext.getUgi();
			String query = hookContext.getQueryPlan().getQueryString();
			notify(query,ugi);
		default:
		
		}
		
	}

	private void notify(String message, UserGroupInformation ugi) {
		Properties p = null;
		producer = new KafkaProducer(p);
		ProducerRecord record = new ProducerRecord("blah", message);
		Future sentResult = producer.send(record);
		//todo
	}
	


}
