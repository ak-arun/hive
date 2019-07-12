package com.ak.hive.ddlgrabber.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.entity.DDLObject;
import com.ak.hive.ddlgrabber.exception.DBException;
import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;
import com.ak.hive.ddlgrabber.util.DDLGrabberUtils;

public class DAO {
	
	private static final Logger LOG = LoggerFactory.getLogger(DAO.class);
	
	PreparedStatement ps = null;

	List<String> dbNames = null;
	List<String> tblNames = null;
	ResultSet rs = null;
	Statement statement = null;
	String ddl = null;

	public  boolean executeInsert(Connection con, List<DDLObject> ddls, String postGresTable) throws DBException{
		try{
			ps = con.prepareStatement(DDLGrabberConstants.INSERT.replace("<tablename>", postGresTable));
			for(DDLObject ddl : ddls){
				ps.setString(1, ddl.getDatabaseName());
				ps.setString(2, ddl.getTableName());
				ps.setString(3, ddl.getDdl());
				ps.setTimestamp(4,ddl.getTimestamp());
				ps.addBatch();
			}
			ps.executeBatch();
			return true;
		}catch (Exception e){
			LOG.info("Exception encountered while batch executeInsert "+DDLGrabberUtils.getTraceString(e));
			throw new DBException(e);
		}
	}
	
	
	public  List<String> getDatabases(Connection con)throws DBException{
		dbNames = new ArrayList<String>();
		try{
			rs = con.createStatement().executeQuery(DDLGrabberConstants.SHOW_DBS);
		while(rs.next()){
			dbNames.add(rs.getString(1));
		}
		}catch(Exception e){
			LOG.info("Exception encountered while getting databases from hive "+DDLGrabberUtils.getTraceString(e));
			throw new DBException(e);
		}
		return dbNames;
	}
	
	public  List<String> getTables(Connection con,String dbName) throws DBException{
		tblNames = new ArrayList<String>();
		try{
			statement = con.createStatement();
			statement.execute(DDLGrabberConstants.USE_DBS.replace("<databasename>", dbName));
			rs = statement.executeQuery(DDLGrabberConstants.SHOW_TBLS);
			while(rs.next()){
				tblNames.add(rs.getString(1));
			}
			
		}catch ( Exception e){
			LOG.info("Exception encountered while getting tables from hive "+DDLGrabberUtils.getTraceString(e));
			throw new DBException(e);
		}
		return tblNames;
		
	}
	
	public  String getDDL(Connection con, String tableName) throws DBException{
		ddl="";
		try{
			statement = con.createStatement();
			rs = statement.executeQuery(DDLGrabberConstants.SHOW_CREATE_TBL.replace("<tablename>", tableName));
			while(rs.next()){
				ddl = ddl+" "+rs.getString(1);
			}
		}catch(Exception e){
			LOG.info("Exception encountered while getting ddls from hive "+DDLGrabberUtils.getTraceString(e));
			throw new DBException(e);
		}
		return ddl;
	}
	
	
	public List<DDLObject> getDBAndTables(Connection con, String metaQuery) throws DBException {
		List<DDLObject> ddlSource = new ArrayList<DDLObject>();
		long ts = System.currentTimeMillis();
		try {
			statement = con.createStatement();
			rs = statement.executeQuery(metaQuery);
			while (rs.next()) {
				ddlSource.add(new DDLObject(rs.getString(1), rs.getString(2),
						"", ts));
			}
		} catch (Exception e) {
			LOG.info("Exception encountered while executing metastore query "+DDLGrabberUtils.getTraceString(e));
			throw new DBException(e);
		}

		return ddlSource;

	}
	
}
