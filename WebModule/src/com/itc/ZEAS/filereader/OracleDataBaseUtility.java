package com.itc.zeas.filereader;

import org.apache.log4j.Logger;

/*
 * This class contains oracle database operations that need to be performed.
 */
public class OracleDataBaseUtility implements IDataBaseUtility {
	
	Logger logger= Logger.getLogger("OracleDataBaseUtility");
	
	String tableName ;

	/**
	 * Enum for mapping oracle data types to existing supported data types. 
	 */
	public enum OracleDataTypeMap {
		
		CHAR("string"), NCHAR("string"), NVARCHAR2("string"), VARCHAR2("string"), LONG("string"),
		NUMBER("double"), NUMERIC("double"), FLOAT("float"), DEC("double"), DECIMAL("double"),
		INTEGER("int"), INT("int"), SMALLINT("int"), REAL("double"), DOUBLE("double"), 
		DATE("date"), TIMESTAMP("timestamp"), CLOB("string"), RAW("notSupported"),
		BFILE("notSupported"), BLOB("notSupported"), NCLOB("notSupported"),
		ROWID("notSupported"), UROWID("notSupported");
	
		private String value;
		    
		OracleDataTypeMap(String value) {
			this.value = value;
		}
	
	    public String getValue() {
	        return this.value;
	    }
	}
	
	/**
	 * Method for getting supported Data type
	 * @return data type
	 */
	public String getSupportedDataType(String dataType) {
    	return OracleDataTypeMap.valueOf(dataType).getValue();
	}
	
	/**
	 * Method for getting describe query
	 * @return query
	 */
	public String getDiscribeQuery(String tableName) {
		this.tableName = tableName;
		String query =  "select * from "+ tableName + " WHERE ROWNUM = 1";
		logger.info("OracleDataBaseUtility: getDiscribeQuery " + query);
		return query;	
	}
	
	/**
	 * Method for getting select query
	 * @return query
	 */
	public String getSelectQuery(String coulmnNameString) {	
		String query =  "select " + coulmnNameString + " from "+ tableName +" WHERE rownum < 10001";	
		logger.info("OracleDataBaseUtility: getSelectQuery " + query);
		return query;	
	}
}

