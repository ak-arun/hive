package com.ak.hive.ddlgrabber.db;

import com.ak.hive.ddlgrabber.entity.DBConfig;

public class PostgresConnectionManager extends ConnectionManager {
	
	public PostgresConnectionManager(DBConfig conf){
		this.conf = conf;
	}
}
