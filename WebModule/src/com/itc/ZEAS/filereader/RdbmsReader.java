package com.itc.zeas.filereader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.itc.taphius.utility.ConfigurationReader;
import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasFileNotFoundException;
import com.itc.zeas.exception.ZeasSQLException;

/*
 * This class connects to RDBMS database based on type and fetch column name, column data type and
 *  first 1000 lines of RDBMS table
 */
public class RdbmsReader implements IFileDataTypeReader {
	
	Logger logger= Logger.getLogger("RdbmsReader");

	private String sampleFileLoc = "";

	private Connection connObject;
	  
	private IDataBaseUtility dbUtility;
	
	Map<String, String> map = new LinkedHashMap<String, String>();	
	
	//to store actual data type of column in  ordered way
	private List<String>  actualDataTypelist;
	
	/**
	 * Method used for initializing the db connection. Also fetches column name and data type
	 * @param filename, dbInfo 
	 * @return map
	 */
	public Map<String, String> getColumnAndDataType(String filename, ExtendedDetails dbInfo) throws ZeasException {

		logger.info("RdbmsReader: START");
			
			// Initializing stuffs needed for data base connection and query execution.
			if (dbInfo != null)
				initialize(dbInfo);
				
				if (connObject != null) {
					try {
					// Create connection and execute describe query for fetching column name and data type.
					String discribeQuery = dbUtility.getDiscribeQuery(dbInfo.getTableName());
					ResultSet rs = DbReaderUtility.executeQuery(connObject, discribeQuery);
					
					// Get Meta data from result set.
					ResultSetMetaData rsmd = rs.getMetaData();
					int numOfCols = rsmd.getColumnCount();
					actualDataTypelist= new ArrayList<>();
					// Iterate through the meta data to get column name and data type.
					for (int i=1; i <= numOfCols; ++i) {		
						String cloumnName = rsmd.getColumnName(i);
						String actulaDataType = rsmd.getColumnTypeName(i);
						
						// Map the data type of column to existing supported data types using Data type map enum.
						String dataType = dbUtility.getSupportedDataType(actulaDataType);	
						if (!dataType.equals(FileReaderConstant.DATATYPE_NOT_SUPPORTED)) {
							map.put(cloumnName, dataType);
							actualDataTypelist.add(actulaDataType.toUpperCase());
						}
					}
				} catch (SQLException ex){
					logger.info("getColumnAndDataType: SQLException: " + ex.getMessage());
					throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, ex.getMessage(), "");
					//ex.printStackTrace();		
				}
				logger.info("getColumnAndDataType: map -> which contains data type column name: "+map);	
			}

		// Catching required exceptions.
		
		return map;
	}

	/**
	 * Method for getting the first 1000 lines from database for preview purpose
	 * @return list
	 */
	public List<List<String>> getColumnValues() throws ZeasException{
		List<List<String>> colList = new ArrayList<>();

		try {
			
			// If the file exists, delete it.
			if (connObject != null ) {
				File file = new File(sampleFileLoc);
				if (file.exists()) { 
					file.delete();
					file = null;
				}
	
				// Get select query and execute query
				String coulmnNameString = getColumnsString();
				String selectQuery = dbUtility.getSelectQuery(coulmnNameString);
				ResultSet rs = DbReaderUtility.executeQuery(connObject, selectQuery);
				FileWriter wr = new FileWriter(sampleFileLoc);
				
				// Get the column values from result set and write into file
			    while (rs.next()) {
			    	for (int i = colList.size(); i <map.size() ; i++) {
						colList.add(new ArrayList<String>());
			    	}
		    		
			    	int j=0;
			    	StringBuilder colData =new StringBuilder("");
			    	
			    	// Get each value from result set and put into list.
			    	for (int i =1; i< (map.size() + 1);i++){
			    		
			    		String actualDataType=actualDataTypelist.get(i-1);
			    		String value="";
			    		if(actualDataType.contains("TEXT") || actualDataType.contains("CLOB")){
			    			Clob clobObject=rs.getClob(i);
			    			if(clobObject !=null) {
					    		 long length=clobObject.length();
					    		 if(length>0 ) {
					    			//read first 10 character from clob data
					    			 if(length<=10) {
					    				 value=clobObject.getSubString(1, (int)length) +" . . . . . ";
					    			 } 
					    			 else{
					    				 value=clobObject.getSubString(1, 10)+" . . . . . ";
					    			 }
					    		 }
					    	  }
					    	  else{
					    		  value=null;
					    	  }
			    		}
			    		else{
			    			value=rs.getString(i);
			    		}
			    		colData.append(value);
			    		if (i != map.size()) {
			    			colData.append(",");
			    		}
			    		colList.get(j).add(value);
			    		j++;
			    	}

			        // Write the content to csv file with , separation.
			    	wr.append(colData);
			    	wr.append("\n");
			    }    
			    
			    // Close file resources.
			    wr.flush();
			    wr.close();
	
				if (connObject != null){
					connObject.close();
					connObject = null;
				}   		
				logger.info("getColumnValues colList -> Contains columns data " + colList);			
				logger.info("RdbmsReader: END");
			}
		
		// Catching required exceptions.
		} catch (SQLException ex) {
			logger.info("getColumnValues: SQLException: " + ex.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, ex.getMessage(), "");		
		} 
		catch (IOException ex) {
			logger.info("getColumnValues: file not found: " + ex.getMessage());
			String error=ex.getMessage();
			if(error.contains(":")){
				error= error.substring(0, error.indexOf(":"));
			}
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,error,"");		
		}

		return colList;
	}
	
	/**
	 * Method for getting connection object, dbutility object and setting file location
	 * @param dbInfo
	 * @throws ZeasException 
	 */
	private void initialize(ExtendedDetails dbInfo) throws ZeasException {

		try {

			// Get db connector and dbutility object based on data base type, by connecting to connection factory.
			DataBaseFactory dbFactory = new  DataBaseFactory();
			IDataBaseConnector dbConnector = dbFactory.getDbConnector(dbInfo);
			dbUtility = dbFactory.getDbUtility();
			
			// Get connection object.
			connObject = dbConnector.getDataBaseConnection();		   
		    sampleFileLoc = ConfigurationReader.getProperty("APP_DIR")+ "/" + dbInfo.getTableName() + FileReaderConstant.SAMPLE_FILE; 
			logger.info("getDbConnection: Sample file location path: "+sampleFileLoc);	

		} catch (ZeasSQLException ex) {
			logger.info("getDbConnection: SQLException: " + ex.toString());
			
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,ex.getError(),"");
			//ex.printStackTrace();
		} 
	}
	
	/**
	 * Method for getting columns names and construct string using columns name
	 * @return columnsName
	 */
	private String getColumnsString() {
		
		String columnsName = "";
		try {
			int count = 0;
			for (String key : map.keySet()) {
				if (count != (map.size() -1)) {
					columnsName += key + ", ";
				} else {
					columnsName += key;
				}
				++count;
			}  
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		logger.info("getColumnsString: " + columnsName);
		return columnsName;
	}
}

			
