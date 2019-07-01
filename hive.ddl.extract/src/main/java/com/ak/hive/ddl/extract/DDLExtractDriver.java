package com.ak.hive.ddl.extract;

public class DDLExtractDriver {

	
	public static void main(String[] args) {
		
		
		HiveDDLExtractor extractor = new HiveDDLExtractor();
		extractor.extract("properties.txt");
		
		
	}
	
	
}
