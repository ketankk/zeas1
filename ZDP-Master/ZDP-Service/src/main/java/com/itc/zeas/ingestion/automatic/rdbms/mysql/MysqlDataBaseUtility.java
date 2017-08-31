package com.itc.zeas.ingestion.automatic.rdbms.mysql;

import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseUtility;
import org.apache.log4j.Logger;

/*
 * This class contains mysql database operations that need to be performed.
 */
public class MysqlDataBaseUtility implements IDataBaseUtility {

	Logger logger = Logger.getLogger("MysqlDataBaseUtility");


	/**
	 * Enum for mapping mysql data types to existing supported data types.
	 */
	public enum MysqlDataTypeMap {

		INT("int"), INTEGER("int"), TINYINT("int"), SMALLINT("int"), MEDIUMINT("int"), BIGINT("long"), FLOAT(
				"float"), DECIMAL("double"), DOUBLE("double"), DATE("date"), DATETIME("timestamp"), TIMESTAMP(
						"timestamp"), YEAR("int"), TIME("string"), CHAR("string"), VARCHAR("string"), TEXT(
								"string"), TINYTEXT("string"), MEDIUMTEXT("string"), LONGTEXT("string"), ENUM(
										"string"), BLOB("notSupported"), TINYBLOB("notSupported"), MEDIUMBLOB(
												"notSupported"), LONGBLOB("notSupported");

		private String value;

		MysqlDataTypeMap(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	/**
	 * Method for getting supported Data type
	 * 
	 * @return data type
	 */
	public String getSupportedDataType(String dataType) {
		return MysqlDataTypeMap.valueOf(dataType).getValue();
	}

	/**
	 * Method for getting describe query
	 * 
	 * @return query
	 */
	public String getDescribeQuery(String tableName) {
		String query = "select * from " + tableName + " limit 1";
		logger.info("MysqlDataBaseUtility: getDiscribeQuery " + query);

		return query;
	}

	/**
	 * Method for getting select query
	 * 
	 * @return query
	 */
	public String getSelectQuery(String coulmnNameString, String tableName) {
		String query = "select " + coulmnNameString + " from " + tableName + " limit 1000";
		logger.info("MysqlDataBaseUtility: getSelectQuery " + query);

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

	/**
	 * Method to get the list of Tables
	 * 
	 * @return query
	 */
	public String getTablesListQuery(String databaseName) {
		
		String query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'base table' AND table_schema='"
				+ databaseName + "'";
		logger.info("MysqlDataBaseUtility: TablesList Query " + query);
		return query;
	}
}