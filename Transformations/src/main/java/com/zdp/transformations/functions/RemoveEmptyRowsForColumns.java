package com.zdp.transformations.functions;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class RemoveEmptyRowsForColumns implements Function<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4272839295681561595L;
	private String[] indexArr;

	public RemoveEmptyRowsForColumns(String[] indexArr) {
		this.indexArr = indexArr;
	}

	
	public String call(String arg0) throws Exception {
		String[] data = arg0.split(",", -1);
		for (Integer i = 0; i < data.length; i++) {
			if (Arrays.asList(indexArr).contains(i.toString()) && (null == data[i] || data[i].isEmpty()
					|| data[i].equalsIgnoreCase("NA") || data[i].equalsIgnoreCase("NULL"))) {
				arg0 = "RemoveIt";
			}
			if (arg0 == "RemoveIt") {
				break;
			}
		}
		// System.out.println("arg :" + arg0);
		return arg0;
	}
}
