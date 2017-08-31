package com.itc.taphius.model;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class PipelineStageLog {
	private int pipelineRunId;
	private String stage;
	private Timestamp runStartTime;
	private Date runEndTime;
	private String status = "failure";
	private String msg;

	public int getPipelineRunId() {
		return pipelineRunId;
	}

	public void setPipelineRunId(int pipelineRunId) {
		this.pipelineRunId = pipelineRunId;
	}

	public String getStage() {
		return stage;
	}

	public void setStage(String stage) {
		this.stage = stage;
	}

	public String getRunStartTime() {
		return  new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(runStartTime);
	}

	public void setRunStartTime(Timestamp runStartTime) {
		this.runStartTime = runStartTime;
	}

	public Date getRunEndTime() {
		return runEndTime;
	}

	public void setRunEndTime(Date runEndTime) {
		this.runEndTime = runEndTime;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}	
	
}
