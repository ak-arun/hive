package com.ak.hive.ddl.extract.db;

import java.sql.Connection;
import java.sql.DriverManager;

import com.ak.hive.ddl.extract.Utils;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.exception.DBException;

public abstract class ConnectionManager {
	
	
	Connection connection = null;
	
	DBConfig conf = null;
	
	public Connection getConnection() throws DBException {
		

		try {
			Class.forName(conf.getDriverClassName());
			connection = DriverManager.getConnection(conf.getConnectString(),
					conf.getUserName(), conf.getPassword());
		} catch (Exception e) {
			throw new DBException(e);
		}

		return connection;
	};

}
