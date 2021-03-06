package com.ak.hive.ddlgrabber.db;

import java.sql.Connection;
import java.sql.DriverManager;

import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.exception.DBException;

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
