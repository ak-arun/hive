package com.ak.hive.ddl.extract;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.ak.hive.ddl.extract.db.ConnectionFactory;
import com.ak.hive.ddl.extract.db.DAO;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.entity.DDLObject;

public class DDLPersist implements Runnable{

	public void run() {
		process();
	}
	
	private List<DDLObject> ddls;
	private DAO dao;
	private List<DDLObject> ddlsProcessed;
	private Connection connectionHive;
	private Connection connectionPostgres;
	private String ddlString;
	private String postGressTable;
	private CountDownLatch latch;
	private DBConfig dbConfig;
	private Connection hiveCon;
	
	
	public DDLPersist(List<DDLObject> ddls, DBConfig dbConfig,Connection connectionPostgres,String postGressTable, CountDownLatch latch ) {
		this.ddls=ddls;
		this.dbConfig=dbConfig;
		this.connectionPostgres=connectionPostgres;
		this.postGressTable=postGressTable;
		this.latch=latch;
	}

	private void process() {
		
		System.out.println("Batch Start  : "+Thread.currentThread().getId()+" "+new Date());
		try{
			hiveCon = new ConnectionFactory(dbConfig).getConnectionManager(Constants.DBTYPE_HIVE).getConnection();
			dao = new DAO();
			System.out.println("Hive Con  : "+Thread.currentThread().getId()+" "+hiveCon+" "+new Date());
			for(DDLObject o : ddls){
				ddlString = dao.getDDL(connectionHive, o.getDatabaseName()+"."+o.getTableName());
				o.setDdl(ddlString);
				ddlsProcessed.add(o);
			}
			latch.countDown();
			System.out.println("Batch End  : "+Thread.currentThread().getId()+" "+new Date());	
			//dao.executeInsert(connectionPostgres, ddlsProcessed, postGressTable);
			hiveCon.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
