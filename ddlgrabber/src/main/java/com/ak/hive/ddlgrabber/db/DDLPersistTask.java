package com.ak.hive.ddlgrabber.db;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.entity.DDLObject;
import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.ak.hive.ddlgrabber.util.DDLGrabberUtils;



public class DDLPersistTask implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(DDLPersistTask.class);

	public void run() {
		persist();
	}
	
	private List<DDLObject> ddls;
	private DAO dao;
	private List<DDLObject> ddlsProcessed;
	private Connection connectionHive;
	private Connection connectionPostgres;
	private String ddlString;
	private String postGresTable;
	private CountDownLatch latch;
	private DBConfig dbConfig;
	
	public DDLPersistTask(List<DDLObject> ddls, DBConfig dbConfig,Connection connectionPostgres,String postGresTable, CountDownLatch latch ) {
		this.ddls=ddls;
		this.dbConfig=dbConfig;
		this.connectionPostgres=connectionPostgres;
		this.postGresTable=postGresTable;
		this.latch=latch;
	}

	private void persist() {
		try{
			connectionHive = new ConnectionFactory(dbConfig).getConnectionManager(DDLGrabberConstants.DBTYPE_HIVE).getConnection();
			System.out.println("Connected to hive "+connectionHive);
			dao = new DAO();
			ddlsProcessed = new ArrayList<DDLObject>();
			for(DDLObject o : ddls){
				ddlString = dao.getDDL(connectionHive, o.getDatabaseName()+"."+o.getTableName());
				o.setDdl(ddlString);
				ddlsProcessed.add(o);
			}
			System.out.println("Processed DDL for batch "+ddlsProcessed.size());
			ddls.clear();
			latch.countDown();
			System.out.println("Executing insert for batch "+ddlsProcessed.size());
			dao.executeInsert(connectionPostgres, ddlsProcessed, postGresTable);
			LOG.info("Persisted "+ddlsProcessed.size()+" table ddls to table "+postGresTable);
			System.out.println("Persisted "+ddlsProcessed.size()+" table ddls to table "+postGresTable);
			connectionHive.close();
		}catch(Exception e){
			e.printStackTrace();
			LOG.info("Exception persisting ddls to table "+DDLGrabberUtils.getTraceString(e));
		}
		
	}

}
