package com.itc.taphius.model;

import java.util.List;

public class DataDisctionaryEntity {
	private String entityName;
	private String entityDescription;
	private List<EntityColumn> entityColumnDesc;
	
	public String getEntityName() {
		return entityName;
	}
	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	public String getEntityDescription() {
		return entityDescription;
	}
	public void setEntityDescription(String entityDescription) {
		this.entityDescription = entityDescription;
	}

	public List<EntityColumn> getEntityColumnDesc() {
		return entityColumnDesc;
	}
	public void setEntityColumnDesc(List<EntityColumn> entityColumnDesc) {
		this.entityColumnDesc = entityColumnDesc;
	}

}
