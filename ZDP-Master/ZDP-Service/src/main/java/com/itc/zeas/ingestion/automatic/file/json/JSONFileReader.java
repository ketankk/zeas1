package com.itc.zeas.ingestion.automatic.file.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderUtility;
import com.itc.zeas.ingestion.automatic.file.parsers.JSONParser;
import org.apache.log4j.Logger;

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
					String dataType= FileReaderUtility.columnDataType(entry.getValue());
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
