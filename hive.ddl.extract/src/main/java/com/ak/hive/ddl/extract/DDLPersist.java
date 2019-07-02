package com.ak.hive.ddl.extract;

import java.sql.Connection;
import java.util.Date;
import java.util.List;

import com.ak.hive.ddl.extract.db.DAO;
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
	
	
	public DDLPersist(List<DDLObject> ddls, DAO dao, Connection connectionHive,Connection connectionPostgres,String postGressTable ) {
		this.ddls=ddls;
		this.dao=dao;
		this.connectionHive=connectionHive;
		this.connectionPostgres=connectionPostgres;
		this.postGressTable=postGressTable;
	}

	private void process() {
		System.out.println("Batch Start  : "+Thread.currentThread().getId()+" "+new Date());
		try{
			for(DDLObject o : ddls){
				ddlString = dao.getDDL(connectionHive, o.getDatabaseName()+"."+o.getTableName());
				o.setDdl(ddlString);
				ddlsProcessed.add(o);
			}
			
			System.out.println("Batch End  : "+Thread.currentThread().getId()+" "+new Date());	
			//dao.executeInsert(connectionPostgres, ddlsProcessed, postGressTable);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
