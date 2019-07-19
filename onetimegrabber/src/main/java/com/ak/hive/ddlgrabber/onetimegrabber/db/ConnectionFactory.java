package com.ak.hive.ddlgrabber.onetimegrabber.db;

import com.ak.hive.ddlgrabber.entity.DBConfig;
import com.ak.hive.ddlgrabber.util.DDLGrabberConstants;



public class ConnectionFactory {
	
	private DBConfig conf=null;

	public ConnectionFactory(DBConfig conf){
		this.conf = conf;
	}

	public ConnectionManager getConnectionManager(String connectionType) {

		if (connectionType != null) {

			if (connectionType.equalsIgnoreCase(DDLGrabberConstants.DBTYPE_MYSQL)) {
				return new MySQLConnectionManager(conf);

			} else if (connectionType
					.equalsIgnoreCase(DDLGrabberConstants.DBTYPE_POSTGRES)) {
				return new PostgresConnectionManager(conf);

			} else if (connectionType.equalsIgnoreCase(DDLGrabberConstants.DBTYPE_HIVE)) {
				return new HiveConnectionManager(conf);
			}
		}
		return null;
	}
}
