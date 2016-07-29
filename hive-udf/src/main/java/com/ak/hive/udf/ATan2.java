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
	
//	public Double evaluate(Double a, Double b) {
//		if(a==null||b==null){
//			return null;
//		}else{
//			try{
//				return (Math.atan2(a,b));
//			}catch(Exception e){
//				e.printStackTrace();
//			}
//			return null;
//		}
//	  }
	
	
//	public static void main(String[] args) {
//		
//		com.gc.hive.udf.ATan2 a = new com.gc.hive.udf.ATan2();
//		System.out.println(a.evaluate(26160.1894683912, 15599.1348085999));
//		
//		System.out.println(a.evaluate(395.067680399865, 309.634808599949));
//		
//		
//	}
}
