package com.ak.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.ak.hive.udf.helper.MathHelper;

public class WidthBucketUDF extends UDF {

	public Long evaluate(Object a, Object b, Object c, Object d) {

		if (a == null || b == null || c == null || d == null) {
			return null;
		} else {
			try {
				double operand = Double.valueOf(a.toString().trim());
				double bound1 = Double.valueOf(b.toString().trim());
				double bound2 = Double.valueOf(c.toString().trim());
				long bucketCount = Long.valueOf(d.toString().trim());
				return MathHelper.getWidthBucket(operand, bound1, bound2, bucketCount);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

		}

	}

}
