package com.ak.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ATan2 extends UDF{

	public Double evaluate(Object a, Object b) {
		if(a==null||b==null){
			return null;
		}else{
			try{
				return Double.valueOf((Math.atan2(Double.valueOf(a.toString().trim()),Double.valueOf(b.toString().trim()))));
			}catch(Exception e){
				e.printStackTrace();
			}
			return null;
		}
	  }
}
