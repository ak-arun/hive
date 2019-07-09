package com.ak.hive.ddlgrabber.db;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.entity.DDLObject;
import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;



public class DDLPersistTask implements Runnable{

	public void run() {
		process();
	}
	
	private List<DDLObject> ddls;
	private DAO dao;
	//private List<DDLObject> ddlsProcessed;
	private Connection connectionHive;
	private Connection connectionPostgres;
	private String ddlString;
	private String postGresTable;
	private CountDownLatch latch;
	private DBConfig dbConfig;
	private PrintWriter printWriter;
	
	public DDLPersistTask(List<DDLObject> ddls, DBConfig dbConfig,Connection connectionPostgres,String postGresTable, CountDownLatch latch ) {
		this.ddls=ddls;
		this.dbConfig=dbConfig;
		this.connectionPostgres=connectionPostgres;
		this.postGresTable=postGresTable;
		this.latch=latch;
	}

	private void process() {
		
		try{
			connectionHive = new ConnectionFactory(dbConfig).getConnectionManager(DDLGrabberConstants.DBTYPE_HIVE).getConnection();
			dao = new DAO();
			printWriter = new PrintWriter(new File("/home/hdfs/"+Thread.currentThread().getId()));
			//ddlsProcessed = new ArrayList<DDLObject>();
			for(DDLObject o : ddls){
				ddlString = dao.getDDL(connectionHive, o.getDatabaseName()+"."+o.getTableName());
				o.setDdl(ddlString);
				printWriter.println(o.toString());
				//ddlsProcessed.add(o);
			}
			latch.countDown();
			//dao.executeInsert(connectionPostgres, ddlsProcessed, postGressTable);
			connectionHive.close();
			printWriter.flush();
			printWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
