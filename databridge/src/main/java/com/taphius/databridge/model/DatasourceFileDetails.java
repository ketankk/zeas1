package com.taphius.databridge.model;

public class DatasourceFileDetails {
 
private String fileName;
private String fileType;
private String noOfColumn;
private String fixedValues;
private String rowDeli;
private String colDeli;
private String dbType;
private String port;
private String hostName;
private String dbName;
private String userName;
private String password;
//added by Deepak to handle First record Header scenario start
private String hFlag;
//added by Deepak to handle First record Header scenario end

 public String getDbType() {
	return dbType;
}
public void setDbType(String dbType) {
	this.dbType = dbType;
}
public String getPort() {
	return port;
}
public void setPort(String port) {
	this.port = port;
}
public String getHostName() {
	return hostName;
}
public void setHostName(String hostName) {
	this.hostName = hostName;
}
public String getDbName() {
	return dbName;
}
public void setDbName(String dbName) {
	this.dbName = dbName;
}
public String getUserName() {
	return userName;
}
public void setUserName(String userName) {
	this.userName = userName;
}
public String getPassword() {
	return password;
}
public void setPassword(String password) {
	this.password = password;
}
public String getRowDeli() {
	return rowDeli;
}
public void setRowDeli(String rowDeli) {
	this.rowDeli = rowDeli;
}
public String getColDeli() {
	return colDeli;
}
public void setColDeli(String colDeli) {
	this.colDeli = colDeli;
}
public String getNoOfColumn() {
	return noOfColumn;
}
public void setNoOfColumn(String noOfColumn) {
	this.noOfColumn = noOfColumn;
}
public String getFixedValues() {
	return fixedValues;
}
public void setFixedValues(String fixedValues) {
	this.fixedValues = fixedValues;
}
public String getFileType() {
	return fileType;
}
public void setFileType(String fileType) {
	this.fileType = fileType;
}
public String getFileName() {
	return fileName;
}
public void setFileName(String fileName) {
	this.fileName = fileName;
}
//added by Deepak to handle First record Header scenario 
public String gethFlag() {
	return hFlag;
}
public void sethFlag(String hFlag) {
	this.hFlag = hFlag;
}
}
