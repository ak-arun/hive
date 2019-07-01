package com.ak.hive.ddl.extract.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.ak.hive.ddl.extract.Constants;
import com.ak.hive.ddl.extract.entity.DDLObject;
import com.ak.hive.ddl.extract.exception.DBException;

public class DAO {
	
	private static PreparedStatement ps = null;
	
	private static List<String> dbNames = null;
	private static List<String> tblNames = null;
	private static ResultSet rs=null;
	private static Statement statement=null;
	private static String ddl=null;

	public static boolean executeInsert(Connection con, List<DDLObject> ddls, String postGressTable) throws DBException{
		try{
			ps = con.prepareStatement(Constants.INSERT.replace("<tablename>", postGressTable));
			for(DDLObject ddl : ddls){
				ps.setString(1, ddl.getDatabaseName());
				ps.setString(2, ddl.getTableName());
				ps.setString(3, ddl.getDdl());
				ps.setTimestamp(4,ddl.getTimestamp());
				ps.addBatch();
			}
			ps.executeLargeBatch();
			return true;
		}catch (Exception e){
			throw new DBException(e);
		}
	}
	
	
	public static List<String> getDatabases(Connection con)throws DBException{
		dbNames = new ArrayList<String>();
		try{
			rs = con.createStatement().executeQuery(Constants.SHOW_DBS);
		while(rs.next()){
			dbNames.add(rs.getString(1));
		}
		}catch(Exception e){
			throw new DBException(e);
		}
		return dbNames;
	}
	
	public static List<String> getTables(Connection con,String dbName) throws DBException{
		tblNames = new ArrayList<String>();
		try{
			statement = con.createStatement();
			statement.execute(Constants.USE_DBS.replace("<databasename>", dbName));
			rs = statement.executeQuery(Constants.SHOW_TBLS);
			while(rs.next()){
				tblNames.add(rs.getString(1));
			}
			
		}catch ( Exception e){
			throw new DBException(e);
		}
		return tblNames;
		
	}
	
	public static String getDDL(Connection con, String tableName) throws DBException{
		try{
			statement = con.createStatement();
			rs = statement.executeQuery(Constants.SHOW_CREATE_TBL.replace("<tablename>", tableName));
			while(rs.next()){
				ddl = rs.getString(1);
				break;
			}
		}catch(Exception e){
			throw new DBException(e);
		}
		return ddl;
	}
	
}
