package com.itc.zeas.filereader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

import com.itc.zeas.exception.ZeasException;

public class JSONFileReader implements IFileDataTypeReader {

	Logger logger= Logger.getLogger("JSONFileReader");
	List<List<String>> colvalues;
	Map<String,String> colNameAndDataType;
	
	public JSONFileReader(){
		this.colvalues= new ArrayList<>();
		colNameAndDataType=new LinkedHashMap<>();
	}
	
	@Override
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails)
			 {
		
		JSONParser jsonSampleParser= new JSONParser(fileName, true);
		jsonSampleParser.start();
		while(true){
			if(!jsonSampleParser.isAlive()){
				
				Map<String,List<String>> colAndValues=jsonSampleParser.getcolNameAndValues();
				for(Entry<String,List<String>> entry :colAndValues.entrySet()){
					String colName=entry.getKey();
					String dataType=FileReaderUtility.columnDataType(entry.getValue());
					colNameAndDataType.put(colName, dataType);
					colvalues.add(entry.getValue());
				}
				break;
			}
		}
		logger.info(colNameAndDataType);
		
		return colNameAndDataType;
	}

	@Override
	public List<List<String>> getColumnValues() {
		// TODO Auto-generated method stub
		logger.info(colvalues);
		return colvalues;
	}
	

}
