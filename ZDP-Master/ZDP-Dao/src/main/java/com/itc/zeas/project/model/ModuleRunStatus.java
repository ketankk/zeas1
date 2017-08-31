package com.itc.zeas.project.model;

import lombok.Data;

import java.util.Date;

@Data
public class ModuleRunStatus {
	private Long moduleId;
	private Integer moduleVersion;
	private String runStatus;
	private Date startTime;
	private Date endTime;
	private String details;


}
