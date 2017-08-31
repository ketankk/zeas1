package com.itc.zeas.filereader;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.filereader.FileReaderUtility;
import com.itc.zeas.filereader.IFileDataTypeReader;

public class XmlFileReader implements IFileDataTypeReader {

	private Map<String, String> colName;
	private List<List<String>> valuList;
	private XmlToCsv toCsvSmall;
	//private XmlToCsv toCsvBig;
	private String fileName;
	private Map<String, List<String>> colNameAndValue;
	private static String xmlEndTag="";
	public XmlFileReader() {
		valuList=new ArrayList<>();
		colName = new LinkedHashMap<>();
		colNameAndValue = new LinkedHashMap<>();
	}

	@Override
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails)
			 {
		this.fileName = fileName;
		if (new File(this.fileName).length() != 0) { // cheaking whether the file is empty or wrong file name.
			toCsvSmall = new XmlToCsv(fileName, true);
			
			// this thread is for getting first 1000 record information as
			// Map<String,List<String>> having key tag name and list of values
			// for particular tag.
			
			Thread t1 = new Thread(toCsvSmall);
			
			//this thread is for writting the sample file along with big file after conveting to csv.
			
			
			t1.start();
			
			// cheacking if thread 1 finished then get the map containing the information after parcinng 1000 records
			//assigning the map to class variable colNameAndValue.
			
			while (true) {
				if (!t1.isAlive()) {
					colNameAndValue = toCsvSmall.getColumnNameAndDataType();
					xmlEndTag=toCsvSmall.getEndTag();
					break;
				}
			}			
			for(Entry<String,List<String>> entry :colNameAndValue.entrySet()){
				String headerName=entry.getKey();
				String dataType=FileReaderUtility.columnDataType(entry.getValue());
				colName.put(headerName, dataType);
				valuList.add(entry.getValue());
			}
			return colName;
		} else
			return colName; // if the file is empty or doesn't exist then return empty map.
	}
	@Override
	public List<List<String>> getColumnValues() {
		
			return valuList;
	}
	public static String getXmlEndTag(){
		return xmlEndTag;
	}
}
