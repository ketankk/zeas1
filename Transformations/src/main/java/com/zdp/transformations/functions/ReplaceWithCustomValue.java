package com.zdp.transformations.functions;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class ReplaceWithCustomValue implements Function<String, String> {

	private static final long serialVersionUID = -1030840122483068953L;
	private Map<String, String> valuesToIndex;
	private String[] indexArr;

	public ReplaceWithCustomValue(Map<String, String> valuesToIndex, String[] indexArr) {
		this.valuesToIndex = valuesToIndex;
		this.indexArr = indexArr;
	}

	
	public String call(String arg0) throws Exception {
		String tmp = "";
		String[] data = arg0.split(",", -1);
		for (Integer i = 0; i < data.length; i++) {
			if (Arrays.asList(indexArr).contains(i.toString()) && (null == data[i] || data[i].isEmpty()
					|| data[i].equalsIgnoreCase("NA") || data[i].equalsIgnoreCase("NULL")) && i == 0) {
				tmp = valuesToIndex.get(i.toString());
			} else if (Arrays.asList(indexArr).contains(i.toString()) && (null == data[i] || data[i].isEmpty()
					|| data[i].equalsIgnoreCase("NA") || data[i].equalsIgnoreCase("NULL"))) {
				tmp = tmp + "," + valuesToIndex.get(i.toString());
			} else if (i == 0) {
				tmp = data[i];
			} else {
				tmp = tmp + "," + data[i];
			}
		}
		return tmp;
	}
}
