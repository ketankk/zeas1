package com.itc.zeas.model;

import lombok.Data;

@Data
public class ModuleSchema {

	private String name;
	private String dataType;
	
	public ModuleSchema(String name, String dataType) {
		this.name = name;
		this.dataType = dataType;
	}

	
}
