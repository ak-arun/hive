package com.ak.hive.ddl.extract;

import java.util.Set;

public class HiveDDLExtractor {

	public void extract(String fileName) {
		
		// get list of all changed tables 
		// for each table extract DDL 
		
		Set<String> tables = getTableSet();
		
		String query="";
		
		for(String t : tables){
			
			query = "show create table "+t;
			
			// execute query against hive
			
		}
		
		// write DDL to file 
		
		
	}

	private Set<String> getTableSet() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	

}
