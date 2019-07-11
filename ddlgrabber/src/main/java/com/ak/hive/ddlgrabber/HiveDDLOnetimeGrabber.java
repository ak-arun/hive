package com.ak.hive.ddlgrabber;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.db.ConnectionFactory;
import com.ak.hive.ddlgrabber.db.DAO;
import com.ak.hive.ddlgrabber.db.DDLPersistTask;
import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.entity.DDLObject;
import com.ak.hive.ddlgrabber.exception.DBException;
import com.google.common.collect.Iterables;

public class HiveDDLOnetimeGrabber {
	
	private static final Logger LOG = LoggerFactory.getLogger(HiveDDLOnetimeGrabber.class);
	
	List<DDLObject> ddls = null;
	
	public static void main(String[] args) throws DBException, FileNotFoundException, IOException, SQLException, InterruptedException {
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		DAO dao = new DAO();
		
		DBConfig confHive = new DBConfig();
		confHive.setPrincipal(properties.getProperty("hive.user.principal"));
		confHive.setKeytab(properties.getProperty("hive.user.keytab"));
		confHive.setConnectString(properties.getProperty("hive.connection.string"));
		confHive.setDriverClassName(properties.getProperty("hive.driver.class"));
		
		DBConfig confMetastore = new DBConfig();
		confMetastore.setUserName(properties.getProperty("meta.db.user.name"));
		confMetastore.setPassword(properties.getProperty("meta.db.user.password"));
		confMetastore.setDriverClassName(properties.getProperty("meta.db.driver.class"));
		confMetastore.setConnectString(properties.getProperty("meta.db.connection.string"));
		
		DBConfig confDestDb = new DBConfig();
		confDestDb.setUserName(properties.getProperty("dest.db.user.name"));
		confDestDb.setPassword(properties.getProperty("dest.db.user.password"));
		confDestDb.setDriverClassName(properties.getProperty("dest.db.driver.class"));
		confDestDb.setConnectString(properties.getProperty("dest.db.connection.string"));
		
		Connection metastoreConnection = new ConnectionFactory(confMetastore).getConnectionManager(properties.getProperty("meta.db.type")).getConnection();
		Connection destinationConnection = new ConnectionFactory(confMetastore).getConnectionManager(properties.getProperty("dest.db.type")).getConnection();
		
		List<DDLObject> ddls = dao.getDBAndTables(metastoreConnection, properties.getProperty("meta.query"));
		
		metastoreConnection.close();
		
		LOG.info("Completed fetching "+ddls.size()+" tables from hive metastore");
		
		int threadCount = Integer.parseInt(properties.getProperty("num.executor"));
		int batchCount = Integer.parseInt(properties.getProperty("max.items.per.batch"));
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
		Iterable<List<DDLObject>> ddlPartitions = Iterables.partition(ddls, batchCount);
		CountDownLatch latch = new CountDownLatch(Iterables.size(ddlPartitions));
		for(List<DDLObject> ddlObjects : ddlPartitions){
			executor.execute(new DDLPersistTask(ddlObjects, confHive, destinationConnection, properties.getProperty("dest.tablename"),latch));
		}
		latch.await();
		executor.shutdown();
		destinationConnection.close();
		LOG.info("Completed loading ddls for table");
		
	}

	
}
