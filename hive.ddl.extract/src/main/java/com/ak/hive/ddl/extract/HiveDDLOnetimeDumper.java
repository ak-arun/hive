package com.ak.hive.ddl.extract;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.ak.hive.ddl.extract.db.ConnectionFactory;
import com.ak.hive.ddl.extract.db.DAO;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.entity.DDLObject;
import com.ak.hive.ddl.extract.exception.DBException;

public class HiveDDLOnetimeDumper {
	
	List<DDLObject> ddls = null;
	
	public static void main(String[] args) throws DBException, FileNotFoundException, IOException, SQLException {
		
		//TODO loggers
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		PrintWriter pw = new PrintWriter(new File(properties.getProperty("ddl.out.file")));
		
		DAO dao = new DAO();
		
		long ts = System.currentTimeMillis();
		
		DBConfig confHive = new DBConfig();
		confHive.setPrincipal(properties.getProperty("hive.user.principal"));
		confHive.setKeytab(properties.getProperty("hive.user.keytab"));
		confHive.setConnectString(properties.getProperty("hive.connection.string"));
		confHive.setDriverClassName(properties.getProperty("hive.driver.class"));
		
		List<DDLObject> ddls = new ArrayList<DDLObject>();
		
		Connection hiveCon = new ConnectionFactory(confHive).getConnectionManager(Constants.DBTYPE_HIVE).getConnection();
		
		for (String dbName : dao.getDatabases(hiveCon)) {

			ddls = new ArrayList<DDLObject>();
			
			for (String tbl : dao.getTables(hiveCon, dbName)) {

				ddls.add(new DDLObject(tbl, dbName, dao.getDDL(hiveCon, dbName
						+ "." + tbl), ts));
			}
			
			for(DDLObject ddl : ddls){
				pw.println(ddl.getDatabaseName()+"~"+ddl.getTableName()+"~"+ddl.getDdl()+"~"+ddl.getTimestamp());
			}
			ddls.clear();
			pw.flush();
			
		}
		
		pw.close();
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
