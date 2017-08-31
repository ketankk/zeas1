package com.itc.zeas.project.model;

import lombok.Data;

@Data
public class HiveSchema {

	String idAndVersion;
	String tableName;
	
	public HiveSchema(String idAndVersion, String tableName) {

		this.idAndVersion = idAndVersion;
		this.tableName = tableName;
	}
	

}
