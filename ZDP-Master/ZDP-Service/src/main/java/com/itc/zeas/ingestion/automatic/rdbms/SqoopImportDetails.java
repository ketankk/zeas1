package com.itc.zeas.ingestion.automatic.rdbms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.model.BulkEntity;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.taphius.dataloader.LoaderUtil;


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
	private  String primaryKey;
	EntityManager entity = new EntityManager();
	private String splitKey="";
	private String SQOOP_SCRIPT_PATH="";
	
	private static final String SHELL_SCRIPT_TYPE = "/bin/bash";

	/**
	 * Method for getting database and target HDFS details using scheduler name, data source and 
	 * destination data set.
	 * @param schedularName
	 * @return details
	 */
	public String[] getDetailsForImport(String schedularName) {
		
		JSONObject jsonObj = null;
		String[] details = new String[4];
		
		try {

			// Gets the entity object by scheduler name.
			jsonObj = getJsonObjectByName(schedularName);
			frequency = jsonObj.getString("frequency");
		    String dataSource = jsonObj.getString("dataSource");
		    String dataset = jsonObj.getString("destinationDataset");
		    
		    // Retrieve json blob from entity using data source field.
		    jsonObj = getJsonObjectByName(dataSource);
		    JSONObject arr =jsonObj.getJSONObject("fileData");
		    String schema = jsonObj.getString("schema");
		    jsonObj = arr;
		    
		    // Setting the database details into extended object.
		    dbType = jsonObj.getString("dbType");
		    userName = jsonObj.getString("userName");		    
		    password = jsonObj.has("password") ? jsonObj.getString("password") : "";
		    hostName = jsonObj.getString("hostName");
		    port = jsonObj.getString("port");
		    dbName = jsonObj.getString("dbName");
		    tableName = jsonObj.getString("tableName");
		    if(dbType.equals(FileReaderConstant.DB2_TYPE))
		    	 		    	tableName=dbName.concat(".").concat(tableName);
		    
		    this.primaryKey="NO";
		    jsonObj = getJsonObjectByName(schema);
		    JSONArray schemaArr = (JSONArray) jsonObj.getJSONArray("dataAttribute");   
		    String coulmnNames = getColumnsName(schemaArr);
		    
		    String query = jsonObj.has("query")? jsonObj.getString("query") : "";
		   
		    // Gets the target location information using data set name.
		    jsonObj = getJsonObjectByName(dataset);
		    String  batchId = LoaderUtil.getBatchID(frequency);
		    String targetPath = jsonObj.getString("location") ;
		    if(targetPath.charAt(targetPath.length()-1)!='/'){
		    	targetPath=targetPath +"/";
			}
		    String dbUrlAndDriver[] = getDbUrl().split(",");
		    String dbUrl=dbUrlAndDriver[0];
		    String driver=dbUrlAndDriver[1];
		    // Add every information into string array to return.
		    
		    // creating sqoop import script file for particular schedular.
		    String rootPathScript=System.getProperty("user.home")+"/zeas/Config/"+schema;
		    File rootPathScriptFile=new File(rootPathScript);
		    if(!rootPathScriptFile.exists()){
		    	rootPathScriptFile.mkdirs();
		    }
		   /* if(isCount){
		    	rootPathScript +="/SqoopEval.sh";
		    }*/
		   // else{
		    	rootPathScript +="/SqoopImport.sh";
		   // }
		    rootPathScriptFile=new File(rootPathScript);
		    if(!rootPathScriptFile.exists()){
		    	rootPathScriptFile.createNewFile();
		    }
		    SQOOP_SCRIPT_PATH=rootPathScript;
		    	details[0] = SHELL_SCRIPT_TYPE;
		    	details[1] = SQOOP_SCRIPT_PATH;
		    	details[2]=batchId;
		    	details[3]=targetPath;
				if (dbUrl != null && userName != null && password != null && tableName != null && targetPath != null) {
					constructSqoopScript(dbUrl, driver, targetPath, query, coulmnNames);
					constructScheudlerSqoopScript(dbUrl, driver, targetPath, query, coulmnNames);
					constructScheudlerSqoopEval(dbUrl, driver);
					
				}
		    	
			
		} catch (Exception e){
			e.printStackTrace();
		}
		
		return details;
	}
	
	private void constructScheudlerSqoopEval(String dbUrl, String driver) {
		String query ="select count(*) from "+tableName;
		
		FileWriter fileWriter=null;
		StringBuilder builder=new StringBuilder();
		try {
			builder.append("sqoop eval ");
			/*if(!dbType.equals(FileReaderConstant.ORACLE_TYPE)){
				//can't find driver in sqoop lib for oracle ..so commenting driver for oracle
			builder.append(" --driver "+driver);
			}*/
			builder.append(" --connect "+dbUrl);
			builder.append(" --username "+userName);
			if(!(password==null || "".equalsIgnoreCase(password))){
				builder.append(" --password "+password);
			}
				builder.append(" --query '"+query+"'");			
			String path  = SQOOP_SCRIPT_PATH.replaceAll("SqoopImport.sh", "SqoopEval.sh");
			fileWriter=new FileWriter(new File(path));
			fileWriter.write(builder.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileWriter!=null){
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		
	}

	private void constructSqoopScript(String dbUrl,String driver,String targetPath, String query, String coulmnNames) {
		FileWriter fileWriter=null;
		StringBuilder builder=new StringBuilder();
		try {
			builder.append("hadoop fs -rmr -skipTrash "+targetPath+System.lineSeparator());
			builder.append("sqoop import ");
			if(!dbType.equals(FileReaderConstant.ORACLE_TYPE)){
				//can't find driver in sqoop lib for oracle ..so commenting driver for oracle
			builder.append(" --driver "+driver);
			}
			builder.append(" --connect "+dbUrl);
			builder.append(" --username "+userName);
			if(!(password==null || "".equalsIgnoreCase(password))){
				builder.append(" --password "+password);
			}
			if(!(query==null || "".equalsIgnoreCase(query))){
				builder.append(" --query '"+query+" where $CONDITIONS' ");
			}else{
				builder.append(" --table "+tableName);
				builder.append(" --columns "+coulmnNames);
			}
			//checking primary key is available or not
			if(!this.primaryKey.equalsIgnoreCase("YES")){
				builder.append(" -m 1 ");
			}else{
				builder.append(" --split-by "+splitKey);
			}
			//builder.append(" --target-dir "+targetPath+"$(date +%Y_%m_%dT%H_%M_%S)");
			
			builder.append(" --target-dir "+targetPath);
			
			fileWriter=new FileWriter(new File(SQOOP_SCRIPT_PATH));
			fileWriter.write(builder.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileWriter!=null){
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		
	}
	private  void constructScheudlerSqoopScript(String dbUrl,String driver,String targetPath, String query, String coulmnNames) {
		FileWriter fileWriter=null;
		StringBuilder builder=new StringBuilder();
		try {
			builder.append("#hadoop fs -rmr -skipTrash "+targetPath+System.lineSeparator());
			builder.append("targetLocation="+targetPath+"$(date +%Y_%m_%dT%H_%M_%S)");	
			builder.append("\n");
			builder.append("sqoop import ");
			if(!dbType.equals(FileReaderConstant.ORACLE_TYPE)){
			builder.append(" --driver "+driver);
			}
			builder.append(" --connect "+dbUrl);
			builder.append(" --username "+userName);
			if(!(password==null || "".equalsIgnoreCase(password))){
				builder.append(" --password "+password);
			}
			if(!(query==null || "".equalsIgnoreCase(query))){
				builder.append(" --query '"+query+" where $CONDITIONS' ");
			}else{
				builder.append(" --table "+tableName);
				builder.append(" --columns "+coulmnNames);
			}
			//checking primary key is available or not
			if(!this.primaryKey.equalsIgnoreCase("YES")){
				builder.append(" -m 1 ");
			}else{
				builder.append(" --split-by "+splitKey);
			}
			//builder.append(" --target-dir "+targetPath+"$(date +%Y_%m_%dT%H_%M_%S)");
			//    "\"Hello\""
			
			builder.append(" --target-dir \"$targetLocation\"");
			builder.append("\n");
			builder.append("hadoop fs -rmr \"$targetLocation\""+"/_SUCCESS");
			
			System.out.println(builder);
			String path  = SQOOP_SCRIPT_PATH.replaceAll("SqoopImport.sh", "SchedulerSqoopImport.sh");
			
			fileWriter=new FileWriter(new File(path));
			fileWriter.write(builder.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileWriter!=null){
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		
	}
	
	private  void constructBulkScheudlerSqoopScript(String dbUrl,String driver,String targetPath, String query, String coulmnNames) {
		FileWriter fileWriter=null;
		StringBuilder builder=new StringBuilder();
		try {
			builder.append("#hadoop fs -rmr -skipTrash "+targetPath+System.lineSeparator());
			builder.append("targetLocation="+targetPath+"output/$(date +%Y_%m_%dT%H_%M_%S)");	
			builder.append("\n");
			builder.append("sqoop import ");
			if(!dbType.equals(FileReaderConstant.ORACLE_TYPE)){
			builder.append(" --driver "+driver);
			}
			builder.append(" --connect "+dbUrl);
			builder.append(" --username "+userName);
			if(!(password==null || "".equalsIgnoreCase(password))){
				builder.append(" --password "+password);
			}
			if(!(query==null || "".equalsIgnoreCase(query))){
				builder.append(" --query '"+query+" where $CONDITIONS' ");
			}else{
				builder.append(" --table "+tableName);
				builder.append(" --columns "+coulmnNames);
			}
			//checking primary key is available or not
			if(!this.primaryKey.equalsIgnoreCase("YES")){
				builder.append(" -m 1 ");
			}else{
				builder.append(" --split-by "+splitKey);
			}
			//builder.append(" --target-dir "+targetPath+"$(date +%Y_%m_%dT%H_%M_%S)");
			//    "\"Hello\""
			
			builder.append(" --target-dir \"$targetLocation\"");
			builder.append("\n");
			builder.append("hadoop fs -rmr \"$targetLocation\""+"/_SUCCESS");
			
			System.out.println(builder);
			String path  = SQOOP_SCRIPT_PATH.replaceAll("SqoopImport.sh", "SchedulerSqoopImport.sh");
			
			fileWriter=new FileWriter(new File(path));
			fileWriter.write(builder.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fileWriter!=null){
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		
	}

	/**
	 * Method for getting Json Object by name
	 * @param name
	 * @return jsonObj
	 */
	public JSONObject getJsonObjectByName(String name) {
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
	
	public JSONObject getJsonBulkObjectByName(String name) {
		BulkEntity bulkEntityObj;
		JSONObject jsonObj = null;
		
		// Get entity by name, json data and json object.
		try {	
			bulkEntityObj= entity.getBulkEntityByName(name);
		    String jsonDataDataSet = bulkEntityObj.getJsonblobDataset();
		    jsonObj  = new JSONObject(jsonDataDataSet);
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonObj;
	}
	
	public JSONObject getJsonBulkObjSourcetByName(String name) {
		BulkEntity bulkEntityObj;
		JSONObject jsonObj = null;
		
		// Get entity by name, json data and json object.
		try {	
			bulkEntityObj= entity.getBulkEntityByName(name);
		    String jsonDataDataSource = bulkEntityObj.getJsonblobSource();
		    jsonObj  = new JSONObject(jsonDataDataSource);
		    
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
	    List<String> primaryKeys=new ArrayList<>();
	    try {
	    	
	    	// Looping through Json array and get column names and construct column string.
	    	for (int i=0; i<schemaArr.length(); i++) {
		    	jsonObj = (JSONObject) schemaArr.get(i);
		    	String name = jsonObj.getString("Name");
		    	 primaryKeys.add(name+":"+(jsonObj.has("primaryKey")?jsonObj.getString("primaryKey"):"NO"));
		    	
		    	if (i != (schemaArr.length()-1)) {
		    		coulmnNames += name + ",";
		    	} else {
		    		coulmnNames += name;
		    	}
		    } 
	    	for(String pKey:primaryKeys){
	    		String key[]=pKey.split(":");
	    		if(key[1].equalsIgnoreCase("YES")){
	    		this.primaryKey="YES";
	    		splitKey=key[0];
	    		break;
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
		//change mysql to lower case at time of ingestion
	    // Based on mysql type, construct db url and set into extended  object.
	    String dbUrl = null;
	    String driver=null;
	    switch (dbType) {
	    	case FileReaderConstant.MYSQL_TYPE:
	    		 driver= "com.mysql.jdbc.Driver";
	    	    dbUrl = "jdbc:mysql://" + hostName + ":" + port +"/" + dbName;
	    	    break;
	    	case FileReaderConstant.ORACLE_TYPE:
	    		driver="oracle.jdbc.driver.oracledriver";
	    	    dbUrl = "jdbc:oracle:thin:@" + hostName + ":" + port +":" + dbName;	
	    	    break;
	    	case FileReaderConstant.DB2_TYPE:
	    		driver="com.ibm.db2.jcc.DB2Jcc";
	    	    dbUrl = "jdbc:db2://" + hostName + ":" + port +"/" + dbName;	
	    	    break;
	    }
	    return dbUrl+","+driver;
	}
    
public String[] getDetailsForBulkSQLImport(String APP_PATH, String JobName) {
		
		JSONObject jsonObj = null;
		JSONObject filedataJsonObj = null;
		JSONObject jsonObjDataAttribute= null;
		JSONObject jsonObjDataSet = null;

		String[] details = new String[4];
		
		try {

			// Gets the entity object by scheduler name.
			jsonObj = getJsonBulkObjSourcetByName(JobName);
			filedataJsonObj=jsonObj.getJSONObject("fileData");
			frequency = filedataJsonObj.getString("FREQUENCY");
		   /* String dataSource = jsonObj.getString("JOB NAME");
		    String dataset = jsonObj.getString("destinationDataset");
		    */
		    // Retrieve json blob from entity using data source field.
		/*    JSONObject arr =jsonObj.getJSONObject("fileData");
		    String schema = jsonObj.getString("schema");
		    jsonObj = arr;*/
		    
		    // Setting the database details into extended object.
		    dbType = filedataJsonObj.getString("SOURCE TYPE");
		    userName = filedataJsonObj.getString("USERNAME");		    
		    password = filedataJsonObj.has("PASSWORD") ? filedataJsonObj.getString("PASSWORD") : "";
		    hostName = filedataJsonObj.getString("HOST");
		    port = filedataJsonObj.getString("PORT");
		    dbName = filedataJsonObj.getString("DATABASE");
		    tableName = filedataJsonObj.getString("TABLE NAME");
		    if(dbType.equals(FileReaderConstant.DB2_TYPE))
		    	 		    	tableName=dbName.concat(".").concat(tableName);
		    
		    this.primaryKey="NO";
		    JSONArray schemaArr = (JSONArray) jsonObj.getJSONArray("dataAttribute");   
		    String coulmnNames = getColumnsName(schemaArr);
		    
		    String query = jsonObj.has("query")? jsonObj.getString("query") : "";
		   
		    // Gets the target location information using data set name.
		    String  batchId = LoaderUtil.getBatchID(frequency);
		    jsonObjDataSet = getJsonBulkObjectByName(JobName);
		    String targetPath = jsonObjDataSet.getString("location") ;
		    if(targetPath.charAt(targetPath.length()-1)!='/'){
		    	targetPath=targetPath +"/";
			}
		    String dbUrlAndDriver[] = getDbUrl().split(",");
		    String dbUrl=dbUrlAndDriver[0];
		    String driver=dbUrlAndDriver[1];
		    // Add every information into string array to return.
		    
		    // creating sqoop import script file for particular schedular.
		
		   // String rootPathScript=System.getProperty("user.home")+"/zeas/Config/Profiles/"+JobName;
		    String rootPathScript=APP_PATH;

		    File rootPathScriptFile=new File(rootPathScript);
		    if(!rootPathScriptFile.exists()){
		    	rootPathScriptFile.mkdirs();
		    }
		   /* if(isCount){
		    	rootPathScript +="/SqoopEval.sh";
		    }*/
		   // else{
		    	rootPathScript +="/SqoopImport.sh";
		   // }
		    rootPathScriptFile=new File(rootPathScript);
		    if(!rootPathScriptFile.exists()){
		    	rootPathScriptFile.createNewFile();
		    }
		    
		    SQOOP_SCRIPT_PATH=rootPathScript;
		    	details[0] = SHELL_SCRIPT_TYPE;
		    	details[1] = SQOOP_SCRIPT_PATH;
		    	details[2]=batchId;
		    	details[3]=targetPath;
				if (dbUrl != null && userName != null && password != null && tableName != null && targetPath != null) {
					constructSqoopScript(dbUrl, driver, targetPath, query, coulmnNames);
					constructBulkScheudlerSqoopScript(dbUrl, driver, targetPath, query, coulmnNames);
					constructScheudlerSqoopEval(dbUrl, driver);
					
				}
		    	
			
		} catch (Exception e){
			e.printStackTrace();
		}
		
		return details;
	}
	
	public static void main(String[] args) {
		SqoopImportDetails details=new SqoopImportDetails();
		//details.constructScheudlerSqoopScript("", "driver", "targetPath", "query", "coulmnNames");
		details.getDetailsForImport("demo_30_Schedular");
	}
}
