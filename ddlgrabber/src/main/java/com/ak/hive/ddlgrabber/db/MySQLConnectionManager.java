package com.ak.hive.ddlgrabber.db;

import com.ak.hive.ddlgrabber.entity.DBConfig;


public class MySQLConnectionManager extends ConnectionManager {
	
	public MySQLConnectionManager(DBConfig conf) {
		this.conf = conf;
	}
}
