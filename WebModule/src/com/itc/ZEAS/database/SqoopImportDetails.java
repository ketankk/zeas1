package com.itc.zeas.database;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.model.Entity;
import com.itc.zeas.filereader.FileReaderConstant;

/*
 * This class gets the database and target HDFS details needed for copying data from sqoop to HDFS.
 */
public class SqoopImportDetails {
   
	private String dbType;   
    private String userName;
    private String password;
    private String hostName;
    private String port;
    private String dbName; 
    private String tableName;
	private String frequency;
	EntityManager entity = new EntityManager();
	
	private static final String SQOOP_SCRIPT_PATH = System.getProperty("user.home")+"/zeas/Config/SqoopImport.sh";
	private static final String SHELL_SCRIPT_TYPE = "/bin/bash";

	/**
	 * Method for getting database and target HDFS details using scheduler name, data source and 
	 * destination data set.
	 * @param schedularName
	 * @return details
	 */
	public String[] getDetailsForImport(String schedularName) {
		
		JSONObject jsonObj = null;
		String[] details = new String[9];
		
		try {

			// Gets the entity object by scheduler name.
			jsonObj = getJsonObjectByName(schedularName);
			frequency = jsonObj.getString("frequency");
		    String dataSource = jsonObj.getString("dataSource");
		    String dataset = jsonObj.getString("destinationDataset");
		    
		    // Retrieve json blob from entity using data source field.
		    jsonObj = getJsonObjectByName(dataSource);
		    JSONArray arr = (JSONArray) jsonObj.getJSONArray("fileData");
		    String schema = jsonObj.getString("schema");
		    jsonObj = (JSONObject) arr.get(0);
		    
		    // Setting the database details into extended object.
		    dbType = jsonObj.getString("dbType");
		    userName = jsonObj.getString("userName");
		    password = jsonObj.getString("password");
		    hostName = jsonObj.getString("hostName");
		    port = jsonObj.getString("port");
		    dbName = jsonObj.getString("dbName");
		    tableName = jsonObj.getString("tableName");
		    
		    jsonObj = getJsonObjectByName(schema);
		    JSONArray schemaArr = (JSONArray) jsonObj.getJSONArray("dataAttribute");   
		    String coulmnNames = getColumnsName(schemaArr);
		    
		    String query = "\"" + coulmnNames + "\"";
		    
		    // Gets the target location information using data set name.
		    jsonObj = getJsonObjectByName(dataset);
		    String  batchId = getBatchID(frequency);
		    String targetPath = jsonObj.getString("location") + "/" + batchId;
		    
		    String dbUrl = getDbUrl();
		    
		    // Add every information into string array to return.
		    if (dbUrl != null && userName != null && password != null && tableName != null && targetPath != null) {		    	
		    	details[0] = SHELL_SCRIPT_TYPE;
		    	details[1] = SQOOP_SCRIPT_PATH;
			    details[2] = dbUrl;
			    details[3] = userName;
			    details[4] = password;
			    details[5] = tableName;
			    details[6] = targetPath;
			    details[7] = query;
			    details[8] = batchId;
		    }

		} catch (Exception e){
			e.printStackTrace();
		}
		
		return details;
	}
	
	/**
	 * Method for getting Json Object by name
	 * @param name
	 * @return jsonObj
	 */
	private JSONObject getJsonObjectByName(String name) {
		Entity entityObj;
		JSONObject jsonObj = null;
		
		// Get entity by name, json data and json object.
		try {	
		    entityObj= entity.getEntityByName(name);
		    String jsonData = entityObj.getJsonblob();
		    jsonObj  = new JSONObject(jsonData);
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonObj;
	}
	
	/**
	 * Method for getting rdbms column names from json array
	 * @param schemaArr
	 * @return coulmnNames
	 */
	private String getColumnsName(JSONArray schemaArr) {
	    String coulmnNames = "";
	    JSONObject jsonObj;
	    
	    try {
	    	
	    	// Looping through Json array and get column names and construct column string.
	    	for (int i=0; i<schemaArr.length(); i++) {
		    	jsonObj = (JSONObject) schemaArr.get(i);
		    	String name = jsonObj.getString("Name");
		    	
		    	if (i != (schemaArr.length()-1)) {
		    		coulmnNames += name + ",";
		    	} else {
		    		coulmnNames += name;
		    	}
		    } 
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }  
	    return coulmnNames;
	}
	
	/**
	 * Method for getting batch id
	 * @param freq
	 * @return batchId
	 */
    public String getBatchID(String freq) {
        String pattern = "YYYYMMDD";
        if(freq.equalsIgnoreCase("hourly"))
          pattern = pattern+"h";
        if((freq.equalsIgnoreCase("daily")) || (freq.equalsIgnoreCase("weekly")|| freq.equalsIgnoreCase("onetime")))
          pattern = pattern+"00";
       
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(new Date());
        
      }
	
	/**
	 * Method for getting db url
	 * @return dbUrl
	 */
	private String getDbUrl() {
		
	    // Based on mysql type, construct db url and set into extended  object.
	    String dbUrl = null;
	    switch (dbType) {
	    	case FileReaderConstant.MYSQL_TYPE:
	    	    dbUrl = "jdbc:mysql://" + hostName + ":" + port +"/" + dbName;
	    	    break;
	    	case FileReaderConstant.ORACLE_TYPE:
	    	    dbUrl = "jdbc:oracle:thin:@" + hostName + ":" + port +":" + dbName;	
	    	    break;
	    }
	    return dbUrl;
	}
}
