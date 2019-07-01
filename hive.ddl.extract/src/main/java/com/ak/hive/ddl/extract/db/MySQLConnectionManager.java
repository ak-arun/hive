package com.ak.hive.ddl.extract.db;

import com.ak.hive.ddl.extract.entity.DBConfig;


public class MySQLConnectionManager extends ConnectionManager {
	
	public MySQLConnectionManager(DBConfig conf) {
		this.conf = conf;
	}
}
