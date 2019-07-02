package com.ak.hive.ddl.extract;

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

import com.ak.hive.ddl.extract.db.ConnectionFactory;
import com.ak.hive.ddl.extract.db.DAO;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.entity.DDLObject;
import com.ak.hive.ddl.extract.exception.DBException;
import com.google.common.collect.Iterables;

public class HiveDDLOnetimeDumper {
	
	List<DDLObject> ddls = null;
	
	
	
	public static void main(String[] args) throws DBException, FileNotFoundException, IOException, SQLException, InterruptedException {
		
		//TODO loggers
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		//PrintWriter pw = new PrintWriter(new File(properties.getProperty("ddl.out.file")));
		
		DAO dao = new DAO();
		
		long ts = System.currentTimeMillis();
		
		DBConfig confHive = new DBConfig();
		confHive.setPrincipal(properties.getProperty("hive.user.principal"));
		confHive.setKeytab(properties.getProperty("hive.user.keytab"));
		confHive.setConnectString(properties.getProperty("hive.connection.string"));
		confHive.setDriverClassName(properties.getProperty("hive.driver.class"));
		
		List<DDLObject> ddls = new ArrayList<DDLObject>();
		
		
		
		
		Connection hiveCon = new ConnectionFactory(confHive).getConnectionManager(Constants.DBTYPE_HIVE).getConnection();
		
		System.out.println("Query Start  : "+Thread.currentThread().getId()+" "+new Date());
		
		for(String db : dao.getDatabases(hiveCon)){
			for (String table : dao.getTables(hiveCon, db)){
				ddls.add(new DDLObject(table, db, "", ts));
			}
		}
		
		System.out.println("Tables Listed  : "+Thread.currentThread().getId()+" "+new Date());
		System.out.println("Total Tables "+ddls.size()+" "+new Date());
		
		int threadCount = Integer.parseInt(properties.getProperty("num.executor"));
		int batchCount = Integer.parseInt(properties.getProperty("max.items.per.batch"));
		
				
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
		
		Iterable<List<DDLObject>> ddlPartitions = Iterables.partition(ddls, batchCount);
		
		CountDownLatch latch = new CountDownLatch(Iterables.size(ddlPartitions));
		
		for(List<DDLObject> ddlObjects : ddlPartitions){
			System.out.println("Executor Trigger "+new Date());
			executor.execute(new DDLPersist(ddlObjects, confHive, null, "",latch));
		}
		
		
		latch.await();
		
		//executor.shutdown();
		
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
