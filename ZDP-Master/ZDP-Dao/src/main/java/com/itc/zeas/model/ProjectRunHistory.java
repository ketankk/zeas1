package com.itc.zeas.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class ProjectRunHistory {
	
	private String projectId;
	private Long id;
	private String version;
	private String name;
	private String status;
	private Timestamp started;
	private Timestamp finished;
	private Long timeElasped;
	private String timeTaken;
	

	
}
