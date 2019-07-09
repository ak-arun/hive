package com.ak.hive.ddlgrabber.exception;

import com.ak.hive.ddl.extract.Utils;

public class DBException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBException (Exception e){
		super(Utils.getTraceString(e));
	}
	
}
