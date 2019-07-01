package com.ak.hive.ddl.extract;

import java.sql.Connection;
import java.sql.SQLException;

import com.ak.hive.ddl.extract.db.ConnectionFactory;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.exception.DBException;

public class Main {

	public static void main(String[] args) throws DBException, SQLException {
		
		DBConfig conf = new DBConfig();
		conf.setConnectString("jdbc:mysql://localhost:3306/hive");
		conf.setDriverClassName("com.mysql.jdbc.Driver");
		conf.setUserName("arun");
		conf.setPassword("");
		ConnectionFactory factory = new ConnectionFactory(conf);
		Connection connection = factory.getConnectionManager("mysql").getConnection();
		System.out.println(connection);
		connection.close();
	}
	
	
}
