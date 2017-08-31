package com.itc.zeas.profile.model;

import java.util.List;
public class BulkEntity extends Entity{

	private String jsonblobSchema;
	private String jsonblobSource;
	private String jsonblobDataset;
	private List<String> bulkNames;
	

	public List<String> getBulkNames() {
		return bulkNames;
	}

	public void setBulkNames(List<String> bulkNames) {
		this.bulkNames = bulkNames;
	}

	
	
	
	public String getJsonblobSchema() {
		return jsonblobSchema;
	}
	public void setJsonblobSchema(String jsonblobSchema) {
		this.jsonblobSchema = jsonblobSchema;
	}
	public String getJsonblobSource() {
		return jsonblobSource;
	}
	public void setJsonblobSource(String jsonblobSource) {
		this.jsonblobSource = jsonblobSource;
	}
	public String getJsonblobDataset() {
		return jsonblobDataset;
	}
	public void setJsonblobDataset(String jsonblobDataset) {
		this.jsonblobDataset = jsonblobDataset;
	}

/*	@Override
	public String toString() {
		return "Entity [name=" + name + ", location=" + location + ", id=" + id + ", type=" + type + ", format="
				+ format + ", isActive=" + isActive + ", schemaType=" + schemaType + ", jsonblob=" + jsonblob
				+ ", frequency=" + frequency + ", userId=" + userId + ", createdBy=" + createdBy + ", updatedBy="
				+ updatedBy + ", dataSourceId=" + dataSourceId + ", dataDisctionaryEntity=" + dataDisctionaryEntity
				+ ", createdDate=" + createdDate + ", updatedDate=" + updatedDate + "]";

	}
	*/
	
}
