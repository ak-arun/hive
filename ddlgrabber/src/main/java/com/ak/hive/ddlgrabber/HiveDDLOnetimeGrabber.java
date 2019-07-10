package com.ak.hive.ddlgrabber;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.ak.hive.ddlgrabber.db.ConnectionFactory;
import com.ak.hive.ddlgrabber.db.DAO;
import com.ak.hive.ddlgrabber.db.DDLPersistTask;
import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.entity.DDLObject;
import com.ak.hive.ddlgrabber.exception.DBException;
import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.google.common.collect.Iterables;

public class HiveDDLOnetimeGrabber {
	
	List<DDLObject> ddls = null;
	
	
	
	public static void main(String[] args) throws DBException, FileNotFoundException, IOException, SQLException, InterruptedException {
		
		//TODO loggers
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		//PrintWriter pw = new PrintWriter(new File(properties.getProperty("ddl.out.file")));
		
		DAO dao = new DAO();
		
		DBConfig confHive = new DBConfig();
		confHive.setPrincipal(properties.getProperty("hive.user.principal"));
		confHive.setKeytab(properties.getProperty("hive.user.keytab"));
		confHive.setConnectString(properties.getProperty("hive.connection.string"));
		confHive.setDriverClassName(properties.getProperty("hive.driver.class"));
		
		List<DDLObject> ddls = new ArrayList<DDLObject>();
		
		
		
		DBConfig confMetastore = new DBConfig();
		confMetastore.setUserName(properties.getProperty("meta.db.user.name"));
		confMetastore.setPassword(properties.getProperty("meta.db.user.password"));
		confMetastore.setDriverClassName(properties.getProperty("meta.db.driver.class"));
		confMetastore.setConnectString(properties.getProperty("meta.db.connection.string"));
		
		Connection metastoreConnection = new ConnectionFactory(confMetastore).getConnectionManager(properties.getProperty("meta.db.type")).getConnection();
		
		dao.getDBAndTables(metastoreConnection, "select TBL_NAME, NAME from TBLS join DBS on TBLS.DB_ID=DBS.DB_ID", ddls);
		
		
		Connection hiveCon = new ConnectionFactory(confHive).getConnectionManager(DDLGrabberConstants.DBTYPE_HIVE).getConnection();
		
		/*System.out.println("Query Start  : "+Thread.currentThread().getId()+" "+new Date());
		
		for(String db : dao.getDatabases(hiveCon)){
			for (String table : dao.getTables(hiveCon, db)){
				ddls.add(new DDLObject(table, db, "", ts));
			}
		}
		*/
		System.out.println("Tables Listed  : "+Thread.currentThread().getId()+" "+new Date());
		System.out.println("Total Tables "+ddls.size()+" "+new Date());
		
		int threadCount = Integer.parseInt(properties.getProperty("num.executor"));
		int batchCount = Integer.parseInt(properties.getProperty("max.items.per.batch"));
		
				
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
		
		Iterable<List<DDLObject>> ddlPartitions = Iterables.partition(ddls, batchCount);
		
		CountDownLatch latch = new CountDownLatch(Iterables.size(ddlPartitions));
		
		for(List<DDLObject> ddlObjects : ddlPartitions){
			System.out.println("Executor Trigger "+new Date());
			executor.execute(new DDLPersistTask(ddlObjects, confHive, null, "",latch));
		}
		
		
		latch.await();
		
		executor.shutdown();
		
		System.out.println("Executor Completed : "+Thread.currentThread().getId()+" "+new Date());
		
		
		
		hiveCon.close();
		
		
		/*
		DBConfig confPg = new DBConfig();
		
		confPg.setUserName(properties.getProperty("dest.db.user.name"));
		confPg.setPassword(properties.getProperty("dest.db.user.password"));
		confPg.setDriverClassName(properties.getProperty("dest.db.driver.class"));
		confPg.setConnectString(properties.getProperty("dest.db.connection.string"));
		
		Connection con = new ConnectionFactory(confPg).getConnectionManager(Constants.DBTYPE_POSTGRES).getConnection();
		DAO.executeInsert(con, ddls, properties.getProperty("dest.tablename"));
		*/
		
		System.out.println("Done");
		
	}

	
}
