package com.ak.hive.ddl.extract.db;

import com.ak.hive.ddl.extract.entity.DBConfig;

public class PostgresConnectionManager extends ConnectionManager {
	
	public PostgresConnectionManager(DBConfig conf){
		this.conf = conf;
	}
}
