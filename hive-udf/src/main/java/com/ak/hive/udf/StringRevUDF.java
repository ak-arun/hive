package com.ak.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class StringRevUDF extends UDF {
	public Text evaluate(final Text s) {
	    if (s == null) { return null; }
	    return new Text(new StringBuffer(s.toString()).reverse().toString().trim());
	  }
}
