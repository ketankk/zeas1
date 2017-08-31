package com.itc.zeas.profile.model;

import com.itc.zeas.project.model.NameAndDataType;
import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public class ExtendedDetails extends SampleData {


	private String dbName;
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getDbType() {
		return dbType;
	}
	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
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
	public String getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getSelectedTable() {
		return selectedTable;
	}
	public void setSelectedTable(List<String> selectedTable) {
		this.selectedTable = selectedTable;
	}
	public String getDatasetlocation() {
		return datasetlocation;
	}
	public void setDatasetlocation(String datasetlocation) {
		this.datasetlocation = datasetlocation;
	}
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public String getDataSchemaType() {
		return dataSchemaType;
	}
	public void setDataSchemaType(String dataSchemaType) {
		this.dataSchemaType = dataSchemaType;
	}
	public String gethFlag() {
		return hFlag;
	}
	public void sethFlag(String hFlag) {
		this.hFlag = hFlag;
	}
	public String getmFlag() {
		return mFlag;
	}
	public void setmFlag(String mFlag) {
		this.mFlag = mFlag;
	}
	public MultipartFile getSchemaFile() {
		return schemaFile;
	}
	public void setSchemaFile(MultipartFile schemaFile) {
		this.schemaFile = schemaFile;
	}
	public String getSelectedTableDetails() {
		return selectedTableDetails;
	}
	public void setSelectedTableDetails(String selectedTableDetails) {
		this.selectedTableDetails = selectedTableDetails;
	}
	public String getTableQueries() {
		return tableQueries;
	}
	public void setTableQueries(String tableQueries) {
		this.tableQueries = tableQueries;
	}
	public List<NameAndDataType> getSelectedColumnDetails() {
		return selectedColumnDetails;
	}
	public void setSelectedColumnDetails(List<NameAndDataType> selectedColumnDetails) {
		this.selectedColumnDetails = selectedColumnDetails;
	}
	public boolean isEncrypted() {
		return isEncrypted;
	}
	public void setEncrypted(boolean isEncrypted) {
		this.isEncrypted = isEncrypted;
	}
	private String dbType;
	private String tableName;
	private String hostName;
	private String port;
	private String userName;
	private String password;
	private String createdBy;
	private String name;
	private List<String> selectedTable;
	private String datasetlocation;
	private String frequency;
	private String dataSchemaType;
	private String hFlag;
	private String mFlag;
	private MultipartFile schemaFile;
	private String selectedTableDetails;
	private String tableQueries;
	private List<NameAndDataType> selectedColumnDetails;
	private boolean isEncrypted;


}
