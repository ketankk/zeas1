package com.taphius.databridge.model;


public class DataSource {
	/**
	 * Name of the DataSet entity
	 */
	private String name;
	/**
	 * Type of the input source File|RDBMS
	 */
	private String type;
	/**
	 * Path from where data needs to be copied
	 */
	private String location;
	/**
	 * Represents input data format ex. <CSV|flat>
	 */
	private String format;
	/**
	 * Description of the dataSet.
	 */
	private String schema;
	
	private DatasourceFileDetails fileData;
	
	
	
	public DatasourceFileDetails getFileData() {
		return fileData;
	}
	public void setFileData(DatasourceFileDetails fileData) {
		this.fileData = fileData;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
}
