package com.itc.zeas.ingestion.automatic.rdbms;

/*
 * This interface helps in mapping oracle data types to supported data types and queries 
 */
public interface IDataBaseUtility {
	
	 String getSupportedDataType(String dataType);

	 String getDescribeQuery(String tableName);
	
	 String getSelectQuery(String coulmnNameString, String tableName);
	
	 String getTablesListQuery(String databaseName);
	
	 String getDupPrefixCheckQuery(String prefix);
}
