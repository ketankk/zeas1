package com.itc.zeas.ingestion.automatic.rdbms.oracle;

import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseUtility;
import org.apache.log4j.Logger;

/*
 * This class contains oracle database operations that need to be performed.
 */
public class OracleDataBaseUtility implements IDataBaseUtility {
	
	Logger logger= Logger.getLogger("OracleDataBaseUtility");
	

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
	public String getDescribeQuery(String tableName) {
		String query =  "select * from "+ tableName + " WHERE ROWNUM = 1";
		logger.info("OracleDataBaseUtility: getDiscribeQuery " + query);
		return query;	
	}
	
	/**
	 * Method for getting select query
	 * @return query
	 */
	public String getSelectQuery(String coulmnNameString, String tableName) {	
		String query =  "select " + coulmnNameString + " from "+ tableName +" WHERE rownum < 10001";	
		logger.info("OracleDataBaseUtility: getSelectQuery " + query);
		return query;	
	}
	
	/**
	 * Method to get the list of Tables
	 * @return query
	 */
	public String getTablesListQuery(String databaseName) {	
		String query =  "SELECT table_name  FROM user_tables";	
		logger.info("OracleDataBaseUtility: TablesList Query " + query);
		return query;	
	}
	
	/**
	 * Method for getting dup Prefix check query
	 * 
	 * @return query
	 */
	public String getDupPrefixCheckQuery(String prefix) {
		String query = "select count(*)as count from entity where NAME LIKE '"+prefix+"_%'";
		logger.info("MysqlDataBaseUtility: getSelectQuery " + query);
		return query;
	}
}

