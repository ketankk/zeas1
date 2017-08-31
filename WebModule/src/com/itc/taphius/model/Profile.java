package com.itc.taphius.model;

import java.util.Date;

/**
 * POJO class representing consolidated information about data source, data
 * set,schema and scheduler
 * 
 * @author 19217
 * 
 */
public class Profile implements Comparable<Profile> {
	private int datasourceid;
	private String name;
	private String schedulerFrequency;
	private String sourcePath;
	private String sourceFormat;
	private String datasetName;
	private String datasetTargetPath;
	private int datasetID;
	private String scheduler;
	private String schemaJsonBlob;
	private String schemaName;
	private String user;// schema creator

	private int schemaId;
	private int scedulerID;
	private String type;
	private String jobStatus;
	//
	private Date schemaModificationDate;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getDataSetName() {
		return datasetName;
	}

	public void setDataSetName(String dataSetName) {
		this.datasetName = dataSetName;
	}

	public String getDataSetTargetPath() {
		return datasetTargetPath;
	}

	public void setDataSetTargetPath(String dataSetTargetPath) {
		this.datasetTargetPath = dataSetTargetPath;
	}

	public String getScheduler() {
		return scheduler;
	}

	public void setScheduler(String scheduler) {
		this.scheduler = scheduler;
	}

	public String getSchemaJsonBlob() {
		return schemaJsonBlob;
	}

	public void setSchemaJsonBlob(String schemaJsonBlob) {
		this.schemaJsonBlob = schemaJsonBlob;
	}

	public String getSourceFormat() {
		return sourceFormat;
	}

	public void setSourceFormat(String sourceFormat) {
		this.sourceFormat = sourceFormat;
	}

	public int getDatasourceid() {
		return datasourceid;
	}

	public void setDatasourceid(int datasourceid) {
		this.datasourceid = datasourceid;
	}

	public int getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(int schemaId) {
		this.schemaId = schemaId;
	}

	public int getDatasetID() {
		return datasetID;
	}

	public void setDatasetID(int datasetID) {
		this.datasetID = datasetID;
	}

	public int getScedulerID() {
		return scedulerID;
	}

	public void setScedulerID(int scedulerID) {
		this.scedulerID = scedulerID;
	}

	public String getType() {
		return type;
	}

	public void setType(String srcType) {
		this.type = srcType;

	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}

	public Date getSchemaModificationDate() {
		return schemaModificationDate;
	}

	public void setSchemaModificationDate(Date schemaModificationDate) {
		this.schemaModificationDate = schemaModificationDate;
	}

	@Override
	public int compareTo(Profile profile) {
		long t1 = this.getSchemaModificationDate().getTime();
		long t2 = profile.getSchemaModificationDate().getTime();
		if (t2 > t1)
			return 1;
		else if (t1 > t2)
			return -1;
		else
			return 0;
	}

	public String getSchedulerFrequency() {
		return schedulerFrequency;
	}

	public void setSchedulerFrequency(String schedulerFrequency) {
		this.schedulerFrequency = schedulerFrequency;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

}
