package com.ak.hive.udf.helper;

public class MathHelper {



	public  static long getWidthBucket(double operand, double bound1, double bound2, long bucketCount) throws Exception {
		checkCondition(bucketCount > 0, "bucketCount must be greater than 0");
		checkCondition(!isNaN(operand), "operand must not be NaN");
		checkCondition(isFinite(bound1), "first bound must be finite");
		checkCondition(isFinite(bound2), "second bound must be finite");
		checkCondition(bound1 != bound2, "bounds cannot equal each other");

		long result = 0;
		double lower = Math.min(bound1, bound2);
		double upper = Math.max(bound1, bound2);
		if (operand < lower) {
			result = 0;
		} else if (operand >= upper) {
			result = addExact(bucketCount, 1l);
		} else {
			result = (long) ((double) bucketCount * (operand - lower) / (upper - lower) + 1);
		}
		if (bound1 > bound2) {
			result = (bucketCount - result) + 1;
		}
		return result;
	}

	public static long addExact(long x, long y) throws Exception {
		long r = x + y;
		if (((x ^ r) & (y ^ r)) < 0) {
			throw new Exception("long overflow");
		}
		return r;
	}

	private static void checkCondition(boolean condition, String message) throws Exception {
		if (!condition)
			throw new Exception(message);
	}

	public static boolean isFinite(double value) {
		 return Math.abs(value) <= Double.MAX_VALUE;
	}

	public static boolean isNaN(double value) {
		return Double.isNaN(value);
	}


	
	
	
}
