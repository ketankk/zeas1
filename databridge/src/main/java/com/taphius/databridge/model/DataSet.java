package com.taphius.databridge.model;


/**
 * This POJO class models the DataSet entity definition from Entity table.
 * @author 16795
 *
 */
public class DataSet {
	/**
	 * Name of the DataSet entity
	 */
	private String name;
	/**
	 * Description of the dataSet.
	 */
	private String description;
	/**
	 * This defines the Target HDFS directory path,
	 * where raw data needs to be copied.
	 */
	private String location;
	/**
	 * Batch structure in "YYYYMMDD"
	 */
	private String batchStructure;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getBatchStructure() {
		return batchStructure;
	}
	public void setBatchStructure(String batchStructure) {
		this.batchStructure = batchStructure;
	}
}
