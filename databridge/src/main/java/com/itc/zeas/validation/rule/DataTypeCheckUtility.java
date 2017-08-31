package com.itc.zeas.validation.rule;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DataTypeCheckUtility {
	private static Boolean status;

	public static boolean checkType(String dataType, String inputValue) {

		switch (dataType.trim().toLowerCase()) {
		
		case "long":
			
		case "int":
			try {
				Long.parseLong(inputValue);
				status = true;
			} catch (NumberFormatException e) {
				status = false;
			}
			break;

		case "double":
			try {
				Double.parseDouble(inputValue);
				status = true;
			} catch (NumberFormatException e) {
				status = false;
			}
			break;
			
		case "date":
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			try {
				formatter.parse(inputValue);
				status = true;
			} catch (ParseException e) {
				status = false;
			}
			break;
			
		default:
			status=true;
			break;
		}
		return status;
	}
public static Map<Integer, String> getcolNumberAndDataTypeMap(Map<String,String> columnNameAndDataType){
	int colNumber=0;
	Map<Integer,String> dataType= new LinkedHashMap<>();
	for(Entry<String,String> entry : columnNameAndDataType.entrySet()) {
		dataType.put(colNumber,entry.getValue());
		colNumber++;
	}
	return dataType;
	
}
}
