package com.ak.hive.ddl.extract;

public class Constants {
	
	
	public static final String DBTYPE_MYSQL = "mysql";
	public static final String DBTYPE_POSTGRES = "postgres";
	public static final String DBTYPE_HIVE = "hive";
	
	public static final String SHOW_DBS="show databases";
	public static final String SHOW_TBLS="show tables";
	public static final String SHOW_CREATE_TBL="show create table <tablename>";
	public static final String INSERT = "insert into <tablename> values (?,?,?,?)";
	public static final String USE_DBS="use <databasename>";
	
	

}
