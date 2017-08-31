package com.itc.zeas.ingestion.automatic.rdbms.db2;

import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseUtility;
import org.apache.log4j.Logger;

public class DB2DataBaseUtility implements IDataBaseUtility {

	Logger logger = Logger.getLogger("DB2DataBaseUtility");

	/**
	 * Enum for mapping db2 data types to existing supported data types.
	 */
	public enum Db2DataTypeMap {
		// change this enum according to db2, not checked for clob,blob,graphics
		// cases

		INT("int"), INTEGER("int"), SMALLINT("int"), BIGINT("long"), NUMERIC("float"), DECFLOAT("float"), DECIMAL(
				"double"), DOUBLE("double"), REAL(
						"float"), DATE("date"), TIMESTAMP("timestamp"), TIME("string"), CHARACTER("string"), VARCHAR(
								"string"), CLOB("string"), BLOB("string"), GRAPHIC("notsupported"), VARGRAPHIC(
										"notsupported"), DBCLOB("notsupported"), BINARY("string"), VARBINARY("string");
		private String value;

		public String getValue() {
			return this.value;
		}

		Db2DataTypeMap(String value) {
			this.value = value;
		}
	}

	/**
	 * Method for getting supported Data type
	 * 
	 * @return data type
	 */
	@Override
	public String getSupportedDataType(String dataType) {
		return Db2DataTypeMap.valueOf(dataType).getValue();
	}

	/**
	 * Method for getting describe query
	 * 
	 * @return query
	 */

	@Override
	public String getDescribeQuery(String tableName) {
		// change according to db2 query
		String query = "select * from " + tableName.toUpperCase() + " limit 1";
		logger.info("Db2DataBaseUtility: getDescribeQuery " + query);
		return query;
	}

	/**
	 * Method for getting select query
	 * 
	 * @return query
	 */
	@Override
	public String getSelectQuery(String coulmnNameString, String tableName) {
		String query = "select " + coulmnNameString + " from " + tableName + " limit 1000";
		logger.info("Db2DataBaseUtility: getSelectQuery " + query);

		return query;
	}

	/**
	 * Method for getting dup Prefix check query
	 * 
	 * @return query
	 */
	@Override
	public String getDupPrefixCheckQuery(String prefix) {

		String query = "select count(*)as count from entity where NAME LIKE '" + prefix + "_%'";
		logger.info("Db2DataBaseUtility: getSelectQuery " + query);
		return query;
	}

	/**
	 * Method to get the list of Tables
	 * 
	 * @return query
	 */
	@Override
	public String getTablesListQuery(String databaseName) {

		String query = "select tabname from syscat.tables where tabschema='" + databaseName.toUpperCase() + "'";
		logger.info("Db2DataBaseUtility: TablesList Query " + query);
		return query;
	}
}