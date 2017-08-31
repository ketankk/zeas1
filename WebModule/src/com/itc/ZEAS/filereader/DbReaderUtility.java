package com.itc.zeas.filereader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasSQLException;
import com.itc.zeas.mr.GenerateValueForDataType;

public class DbReaderUtility {
	
	static Logger logger= Logger.getLogger("DbReaderUtility");
	
	/**
	 * Method for executing query
	 * @param  connObject, query
	 * @return rsObj
	 */
	public static ResultSet executeQuery(Connection connObject, String query) throws ZeasSQLException{
		Statement stmt;
		ResultSet rsObj = null;
		
		try {
			
			if (connObject != null && query != null) {
				logger.info("Query to be Execute: "+ query);	
		
				// Execute Query
				stmt = connObject.createStatement();
				rsObj = stmt.executeQuery(query);
				
				logger.info("Query to be Execute: "+ rsObj);	
			}
			
		} catch (SQLException e){
//			String error=e.getMessage();
//			if(error.contains(":")){
//				error= error.substring(0, error.indexOf(":"));
//			}
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, e.getMessage(), "");
		} 
		
		return rsObj;
		
	}

	/*
	 * This method returns the column name and their values for given query(query is applicable for single record output)
	 */
	public static Map<String, String> getDBColumnNameAndValue(Connection conn,
			String singleRecordQuery) throws ZeasSQLException {
		
		logger.info("getDBColumnNameAndValue started with query :"+singleRecordQuery);
		Statement statement = null;
		ResultSet resultSet = null;
		Map<String, String> nameAndValue = new HashMap<>();
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(singleRecordQuery);
			int columnCount = resultSet.getMetaData().getColumnCount();
			List<String> columnNames = new ArrayList<>();
			ResultSetMetaData metadata = resultSet.getMetaData();
			for (int index = 1; index <= columnCount; index++) {
				columnNames.add(metadata.getColumnName(index));
			}
			System.out.println(columnNames);
			while (resultSet.next()) {
				for (String colName : columnNames) {
					nameAndValue.put(colName, resultSet.getString(colName));
				}
				logger.info("query executed successfully with output :"+nameAndValue);
			}

		} catch (SQLException e) {
			logger.error("error found while reading column and their value:"+e.toString());
			String error=e.getMessage();
			if(error.contains(":")){
				error= error.substring(0, error.indexOf(":"));
			}
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, error, "");
			
		} finally {

			try {
				if (resultSet != null)
					resultSet.close();
//				if (conn != null)
//					conn.close();
			} catch (SQLException e) {
				logger.error("error found while reading column and their value:"+e.toString());
			}
		}

		return nameAndValue;
	}
	
	/*
	 * this method returns sample file contents on the basis of given schema.
	 */
	public static String getSampleContentForSchema(Map<String,String> colNameAndValue){
		
		logger.info("getSampleContentForSchema started with schema:"+colNameAndValue);
		StringBuilder sampleFileContent= new StringBuilder();
		GenerateValueForDataType valueForDataType= new GenerateValueForDataType();
		int noOfLine=10;
		int columnSize=colNameAndValue.size();
		for(int count=1;count<=noOfLine;count++){
			int c=1;
			for(Entry<String,String> entry :colNameAndValue.entrySet()){
				sampleFileContent.append(valueForDataType.getDummyVlaueForDataType(entry.getValue()));
				if(c<columnSize)
					sampleFileContent.append(",");
				c++;
			}
			sampleFileContent.append("\n");
		}
		
		logger.info("getSampleContentForSchema finished with contents:"+sampleFileContent.toString());
		return sampleFileContent.toString();
	}
}

