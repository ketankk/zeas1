package com.itc.zeas.project.model;

public class NameAndDataType {
	String name;
	String dataType;
	String primaryKey;
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	@Override
	public String toString() {
		return "NameAndDataType [name=" + name + ", dataType=" + dataType + ", primaryKey=" + primaryKey + "]";
	}
	
	

}
