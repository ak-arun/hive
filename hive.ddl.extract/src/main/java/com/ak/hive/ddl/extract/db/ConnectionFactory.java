package com.ak.hive.ddl.extract.db;

import com.ak.hive.ddl.extract.Constants;
import com.ak.hive.ddl.extract.entity.DBConfig;

public class ConnectionFactory {
	
	private DBConfig conf=null;

	public ConnectionFactory(DBConfig conf){
		this.conf = conf;
	}

	public ConnectionManager getConnectionManager(String connectionType) {

		if (connectionType != null) {

			if (connectionType.equalsIgnoreCase(Constants.DBTYPE_MYSQL)) {
				return new MySQLConnectionManager(conf);

			} else if (connectionType
					.equalsIgnoreCase(Constants.DBTYPE_POSTGRES)) {
				return new PostgresConnectionManager(conf);

			} else if (connectionType.equalsIgnoreCase(Constants.DBTYPE_HIVE)) {
				return new HiveConnectionManager(conf);
			}
		}
		return null;
	}
}
