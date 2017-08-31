package com.itc.zeas.profile.model;



public class BulkProfile {

	private int id;
	//private String schemaName;
	private String jobId;//jobname
	private String createdby;

	private int permissionLevel;
	private String jsondata_Source;
//	private String jsondata_Schema;
	//private String jsondata_Dataset;
	private String source;//jonname_source
	private String sourceType;//csv,oracle,mysql
	private String dataset;//location
	private String lastModifiedDate;
	
	
	public String getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(String lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	private String requestedDate;
	

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	public String getCreatedby() {
		return createdby;
	}

	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	

	public String getRequestedDate() {
		return requestedDate;
	}

	public void setRequestedDate(String requestedDate) {
		this.requestedDate = requestedDate;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	public String getJsondata_Source() {
		return jsondata_Source;
	}

	public void setJsondata_Source(String jsondata_Source) {
		this.jsondata_Source = jsondata_Source;
	}

	
	public int getPermissionLevel() {
		return permissionLevel;
	}

	public void setPermissionLevel(int permissionLevel) {
		this.permissionLevel = permissionLevel;
	}

	

}
