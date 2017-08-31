package com.itc.zeas.filereader;

/*
 * This interface helps in mapping oracle data types to supported data types and queries 
 */
public interface IDataBaseUtility {
	
	public String getSupportedDataType(String dataType);

	public String getDiscribeQuery(String tableName); 
	
	public String getSelectQuery(String coulmnNameString);
}
