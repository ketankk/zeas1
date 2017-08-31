package com.itc.zeas.ingestion.model;

public class ZDPRunLogDetails {
	
	private String id;
	private String name;
	private String type;
	private String status;
	private String created_by;
	private String logfilelocation;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
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
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getCreated_by() {
		return created_by;
	}
	public void setCreated_by(String created_by) {
		this.created_by = created_by;
	}
	public String getLogfilelocation() {
		return logfilelocation;
	}
	public void setLogfilelocation(String logfilelocation) {
		this.logfilelocation = logfilelocation;
	}

}
