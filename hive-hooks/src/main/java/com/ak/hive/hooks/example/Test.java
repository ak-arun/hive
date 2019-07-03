package com.ak.hive.hooks.example;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Test {
	
	
	public static void main(String[] args) {
		
		Configuration conf = new PropertiesConfiguration();
		conf.setProperty("a.b.hello1", "");
		conf.setProperty("a.b.hello2", "");
		conf.setProperty("a.b.hello3", "");
		conf.setProperty("a.b.hello4", "");
		conf.setProperty("a.b.hello5", "");
		conf.setProperty("x.y.hello5", "");
		
		
		Configuration blah = conf.subset("a.b");
		Iterator keys = blah.getKeys();
		while (keys.hasNext()) {
			System.out.println(keys.next());
			
		}
		
		
		
	}

}
