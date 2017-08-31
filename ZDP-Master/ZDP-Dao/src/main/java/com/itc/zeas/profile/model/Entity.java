package com.itc.zeas.profile.model;

import java.util.Date;
import java.util.List;

import com.itc.zeas.model.DataDisctionaryEntity;
import lombok.Data;

@Data
public class Entity {

	private String name;
	private String location;
	private long id;
	private String type;
	private String format;
	private boolean isActive;
	private String schemaType;
	private String jsonblob;
	//
	private List<List<Object>> jsonblobForBulk;
	private List<String> bulkNames;
	private String frequency;
	private int userId;
	private String createdBy;
	private String updatedBy;
	private String dataSourceId;
	private DataDisctionaryEntity dataDisctionaryEntity;
	private Date createdDate;
	private Date updatedDate;
//this one for databridge entity
	private String version;


}
