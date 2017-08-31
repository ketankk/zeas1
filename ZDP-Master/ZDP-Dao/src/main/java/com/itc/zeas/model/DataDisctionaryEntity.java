package com.itc.zeas.model;

import com.itc.zeas.ingestion.model.EntityColumn;
import lombok.Data;

import java.util.List;

@Data
public class DataDisctionaryEntity {
	private String entityName;
	private String entityDescription;
	private List<EntityColumn> entityColumnDesc;
	

}
