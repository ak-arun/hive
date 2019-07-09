package com.ak.hive.ddlgrabber.entity;

public class DBConfig {

	public String getDriverClassName() {
		return driverClassName;
	}
	public void setDriverClassName(String driverName) {
		this.driverClassName = driverName;
	}
	public String getConnectString() {
		return connectString;
	}
	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getKeytab() {
		return keytab;
	}
	public void setKeytab(String keytab) {
		this.keytab = keytab;
	}
	public String getPrincipal() {
		return principal;
	}
	public void setPrincipal(String principal) {
		this.principal = principal;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	private String driverClassName;
	private String connectString;
	private String userName;
	private String password;
	private String keytab;
	private String principal;
	private String tableName;
	
	
	
}
