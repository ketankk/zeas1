package com.itc.zeas.model;

import lombok.Data;

import java.sql.Date;
import java.sql.Timestamp;

@Data
public class PipelineStageLog {
	private int pipelineRunId;
	private String stage;
	private Timestamp runStartTime;
	private Date runEndTime;
	private String status = "failure";
	private String msg;

	
}
