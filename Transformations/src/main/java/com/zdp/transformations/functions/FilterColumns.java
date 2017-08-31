package com.zdp.transformations.functions;

import org.apache.spark.api.java.function.Function;

public class FilterColumns implements Function<String, String> {

	private int[] columnIndex;

	public FilterColumns(int[] columnIndex) {
		super();
		this.columnIndex = columnIndex;
	}

	public int[] getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(int[] columnIndex) {
		this.columnIndex = columnIndex;
	}

	private static final long serialVersionUID = -1585427441930984352L;

	public String call(String line) {
		String[] parts = line.split(",");
		String temp = "";
		for (int i = 0; i < columnIndex.length; i++) {
			temp = temp + parts[columnIndex[i]] + ",";
		}
		return temp.substring(0,temp.length()-1);		
	}
}