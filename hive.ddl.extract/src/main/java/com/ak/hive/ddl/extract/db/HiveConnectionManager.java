package com.ak.hive.ddl.extract.db;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import com.ak.hive.ddl.extract.Utils;
import com.ak.hive.ddl.extract.entity.DBConfig;
import com.ak.hive.ddl.extract.exception.DBException;

public class HiveConnectionManager extends ConnectionManager {

	Configuration hadoopConf = null;

	public HiveConnectionManager(DBConfig conf) {
		this.conf = conf;
	}

	public Connection getConnection() throws DBException {
		// TODO implement non krb connection

		try {
			hadoopConf = new Configuration();
			hadoopConf.set("hadoop.security.authentication", "Kerberos");
			UserGroupInformation.setConfiguration(hadoopConf);
			UserGroupInformation.loginUserFromKeytab(conf.getPrincipal(),
					conf.getKeytab());
			Class.forName(conf.getDriverClassName());
			connection = DriverManager.getConnection(conf.getConnectString());
		} catch (Exception e) {
			throw new DBException(e);
		}

		return connection;
	}

}
