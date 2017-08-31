package com.itc.zeas.filereader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;


/**
 * 
 * Description: file reader utility.
 * @author: 18947
 * 
 * Version: 
 * 
 * Date: 25-Mar-2015
 */
public class FileReaderUtility {
	
	private static Logger  logger= Logger.getLogger ( "FileReaderUtility");
	
	/** 
	 *This method returns the data type of column.
	 * 
	 * @author: 18947
	 * Date: 25-Mar-2015
	 * 
	 * @param colValues
	 * @return 
	 */
	public static String columnDataType(List<String> colValues) {

		logger.info ("start columnDataType method , column values size :"+colValues.size());
		String type = "";
		for (String colVal : colValues) {
			colVal = colVal.trim();
			if (!(colVal.equalsIgnoreCase("NE") || colVal
					.equalsIgnoreCase("NULL") || colVal.equals("\\N"))) {
				if (isNumeric(colVal)) {
					if (colVal.contains(".")) {
						type = FileReaderConstant.DATATYPE_DOUBLE;
					} else {
						if (colVal.length() > FileReaderConstant.INT_RANGE ) {
							type = FileReaderConstant.DATATYPE_LONG;
						} else {
							if (!(FileReaderConstant.DATATYPE_LONG.equalsIgnoreCase(type) || 
									FileReaderConstant.DATATYPE_DOUBLE.equalsIgnoreCase(type))) {
								type = FileReaderConstant.DATATYPE_INT;
							}
						}
					}
				} else if (isDate(colVal)) {
					if (colVal.length() >= 10 && isTimestamp(colVal.substring(10).trim())) {
						type = FileReaderConstant.DATATYPE_TIMESTAMP;
					} else {
						if (!FileReaderConstant.DATATYPE_TIMESTAMP.equals(type)) {
							type = FileReaderConstant.DATATYPE_DATE;
						}
					}
					
				} else {
					type = FileReaderConstant.DATATYPE_STRING;
					break;
				}
			}
		}
		logger.info ("successfully find the data type of column :" +type);
		return type;
	}

	
	
	/**
	 * 
	 * This method returns column header with data type
	 * 
	 * @author: 18947
	 * Date: 24-Mar-2015
	 * 
	 * @param headerValues
	 * @param columnValues
	 * @return
	 */
	public static Map<String, String> getColumnNameAndDataType(
			String[] headerValues, Map<String, String> columnValues) {
		logger.info("start the getColumnNameAndDataType, column values with header "+columnValues);
		int headerProbabilityCount = 0;
		int index = 1;
		List<Integer> listOfIndexForNonHeader = new ArrayList<>();
		boolean isMatchedHeader=false;
		Map<String,String> headerDatatypeMap=getHeaderDatatypes(headerValues);
		isMatchedHeader=validateHeaderDatatype(headerDatatypeMap, columnValues);
		boolean isAllStringDatatype =validateStringDatatype(headerDatatypeMap,columnValues);
		System.out.println(isMatchedHeader);
		if(isMatchedHeader  && !isAllStringDatatype){
			return columnValues;
		}
		
		if(!isAllStringDatatype(headerDatatypeMap) && !isAllStringDatatype) {
			return columnValues;
		}
		boolean isHeaderProbable=true;
		if(isMatchedHeader && isAllStringDatatype){
			headerProbabilityCount=0;
			for (String str : headerValues) {
					if (isHeaderName(str)) {
						headerProbabilityCount++;
					}
			}
			if((headerValues.length==1 && headerProbabilityCount==1) || 
					(headerValues.length==2 && headerProbabilityCount==1) ||
					(headerValues.length==3 && headerProbabilityCount>=1)){
				
			}
			else{
				isHeaderProbable=false;
				return columnValues;
			}
		}
		
		for (String str : headerValues) {
		
			try {
				Double.parseDouble(str);
				listOfIndexForNonHeader.add(index);

			} catch (NumberFormatException e) {
				if (isHeaderName(str)) {
					headerProbabilityCount++;
				}
			}
			index++;
		}

		Map<String, String> finalColNameAndType = new LinkedHashMap<>();
		if (isHeaderProbable) {

			for (int i = 1; i <= columnValues.size(); i++) {
				String dataType = columnValues
						.get(FileReaderConstant.DEFAULT_COLUMN + i);
				if (listOfIndexForNonHeader.contains(i)) {
					finalColNameAndType.put(FileReaderConstant.DEFAULT_COLUMN
							+ i, dataType);
				} else {
					if (headerValues.length >= i) {
						String val = headerValues[i - 1];
						if (val.trim().isEmpty()) {
							val = FileReaderConstant.DEFAULT_COLUMN + i;
						}
						finalColNameAndType.put(val, dataType);
					} else {
						finalColNameAndType
								.put(FileReaderConstant.DEFAULT_COLUMN + i,
										dataType);
					}
				}

			}
		} else {
			finalColNameAndType = columnValues;
		}
		logger.info("successfully return datatype and column+"+finalColNameAndType);
		return finalColNameAndType;
	}


	/**
	 * 
	 * it evaluates  given string is numeric type or not.
	 * 
	 * @author: 18947
	 * Date: 24-Mar-2015
	 * 
	 * @param val
	 * @return
	 */
	private static boolean isNumeric(String val) {
		try {
			Double.parseDouble(val.trim());
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	/**
	 * 
	 *  it evaluates  given string is date type or not
	 * 
	 * @author: 18947
	 * Date: 25-Mar-2015
	 * 
	 * @param str
	 * @return
	 */
	private static boolean isDate(String str) {
		str= str.replaceAll("/", "-");
		if (str.matches("\\d{4}-\\d{2}-\\d{2}[a-zA-Z0-9: ]*"))
	        return true;
	    else
	    	return false;
	}
	
	/*
	 * Ít helps to detect  timestamp  like 2011-09-12 12:23:00.
	 */
	private static boolean isTimestamp(String str) {
		if (str.matches("\\d{2}:\\d{2}[a-zA-Z0-9: ]*"))
	        return true;
	    else
	    	return false;
	}
	

	/**
	 * 
	 *  It helps to detect the header related information.
	 * 
	 * @author: 18947
	 * Date: 24-Mar-2015
	 * 
	 * @param str
	 * @return
	 */
	private static boolean isHeaderName(String str) {

		boolean isHeader = false;
		if (str.toLowerCase().contains("id")
				|| str.toLowerCase().contains("name")
				|| str.toLowerCase().contains("cost")
				|| str.toLowerCase().contains("price")
				|| str.toLowerCase().contains("type")
				|| str.toLowerCase().contains("code")
				|| str.toLowerCase().contains("status")
				|| str.toLowerCase().contains("date")
				|| str.toLowerCase().contains("salary")
				|| str.toLowerCase().contains("department")
				|| str.toLowerCase().contains("product")
				|| str.toLowerCase().contains("dept")
				|| str.toLowerCase().contains("address")
				|| str.toLowerCase().contains("state")
				|| str.toLowerCase().contains("category")
				|| str.toLowerCase().contains("loc")
				|| str.toLowerCase().contains("gender")
				|| str.toLowerCase().contains("age")) {
			isHeader = true;
		}
		return isHeader;
	}
	
 
	//get data type for header
	private static Map<String,String> getHeaderDatatypes(String[] headers) {
		
		Map<String,String> headerMap= new LinkedHashMap<>();
		Integer count=1;
		for (String str : headers) {
			List<String> headerValue=new ArrayList<>();
			headerValue.add(str);
			if ((str.equalsIgnoreCase("NE") || str
					.equalsIgnoreCase("NULL") || str.equals("\\N") || str.isEmpty())) {
				headerMap.put(count.toString(), "");
			}
			else{
				headerMap.put(count.toString(), columnDataType(headerValue));
			}
			count++;
		}
		return headerMap;
	}
	
	
	private static boolean validateHeaderDatatype(Map<String,String> headerMap,Map<String,String> dataMap) {
		
		boolean isEqual=true;
		List<String> headerValues=getValues(headerMap);
		List<String> dataValues=getValues(dataMap);
		for(int count=0;count<dataValues.size();count++){
			if(!headerValues.get(count).isEmpty()) {
				String hVal=getType(headerValues.get(count));
				String dVal=getType(dataValues.get(count));
				if (!dVal.equals(hVal)) {
					isEqual = false;
					break;
				}
			}
		}
		return isEqual;
	}
	
	private static List<String> getValues(Map<String,String> map){
		
		List<String> values= new ArrayList<>();
		for(Entry<String, String> entry :map.entrySet()){
			values.add(entry.getValue());
		}
		return values;
		
	}
	
	private static String getType(String value) {

		switch (value) {

		case FileReaderConstant.DATATYPE_INT:
		case FileReaderConstant.DATATYPE_LONG:
		case FileReaderConstant.DATATYPE_DOUBLE:
			value = FileReaderConstant.DATATYPE_INT;
			break;
		case FileReaderConstant.DATATYPE_DATE:
		case FileReaderConstant.DATATYPE_TIMESTAMP:
			value = FileReaderConstant.DATATYPE_DATE;
			break;
		}
		return value;
	}
	
	private static boolean validateStringDatatype(Map<String, String> headerDatatypeMap,Map<String, String> columnValues) {

		boolean isStringType=true;
		for(Entry<String, String> entry :columnValues.entrySet()){
			if(!entry.getValue().equalsIgnoreCase(FileReaderConstant.DATATYPE_STRING)){
				isStringType=false;
			}
		}
		return isStringType;
	}

	private static boolean isAllStringDatatype(Map<String, String> headerDatatypeMap) {

		boolean isStringType=true;
		for(Entry<String, String> entry :headerDatatypeMap.entrySet()){
			if(!entry.getValue().isEmpty()){
			if(!entry.getValue().equalsIgnoreCase(FileReaderConstant.DATATYPE_STRING)){
				isStringType=false;
			}
			}
		}
		return isStringType;
	}

	
}
