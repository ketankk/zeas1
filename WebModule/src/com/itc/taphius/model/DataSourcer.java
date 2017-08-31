package com.itc.taphius.model;

public class DataSourcer {
	private int id;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getDataSourceId() {
		return dataSourceId;
	}
	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getSchemaType() {
		return schemaType;
	}
	public void setSchemaType(String schemaType) {
		this.schemaType = schemaType;
	}
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public String getLastUpdated() {
		return lastUpdated;
	}
	public void setLastUpdated(String lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getJsonblob() {
		return jsonblob;
	}
	public void setJsonblob(String jsonblob) {
		this.jsonblob = jsonblob;
	}
	
	private int userId;
	private String jsonblob;
	
	private String name;
	private String type;
	private DataDisctionaryEntity dataDisctionaryEntity;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public DataDisctionaryEntity getDataDisctionaryEntity() {
		return dataDisctionaryEntity;
	}
	public void setDataDisctionaryEntity(DataDisctionaryEntity dataDisctionaryEntity) {
		this.dataDisctionaryEntity = dataDisctionaryEntity;
	}
	private String location;
	private String format;
	
	public String getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	private boolean isActive;
	private String createdBy;
	private String lastUpdated;
	private String dataSourceId;
	private String schemaType;
	private String frequency;

	}
