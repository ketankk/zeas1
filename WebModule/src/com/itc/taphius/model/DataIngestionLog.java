package com.itc.taphius.model;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class DataIngestionLog {
	
int logId;
String batch;
Timestamp startTime;
Timestamp endTime;
String stage;
String status;
String jobMessage;
Timestamp created;
String createdBy;
Timestamp lastModified;
String updatedBy;

public int getLogId() {
	return logId;
}
public void setLogId(int logId) {
	this.logId = logId;
}

public String getBatch() {
	return batch;
}
public void setBatch(String batch) {
	this.batch = batch;
}
public String getStartTime() {
	return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(startTime);
}
public void setStartTime(Timestamp startTime) {
	this.startTime = startTime;
}
public Timestamp getEndTime() {
	return endTime;
}
public void setEndTime(Timestamp endTime) {
	this.endTime = endTime;
}
public String getStatus() {
	return status;
}
public void setStatus(String status) {
	this.status = status;
}
public String getJobMessage() {
	return jobMessage;
}
public void setJobMessage(String jobMessage) {
	this.jobMessage = jobMessage;
}
public Timestamp getCreated() {
	return created;
}
public void setCreated(Timestamp created) {
	this.created = created;
}
public String getCreatedBy() {
	return createdBy;
}
public void setCreatedBy(String createdBy) {
	this.createdBy = createdBy;
}
public Timestamp getLastModified() {
	return lastModified;
}
public void setLastModified(Timestamp lastModified) {
	this.lastModified = lastModified;
}
public String getUpdatedBy() {
	return updatedBy;
}
public void setUpdatedBy(String updatedBy) {
	this.updatedBy = updatedBy;
}
int dataIngestionId;
/**
 * @return the dataIngestionId
 */
public int getDataIngestionId() {
    return dataIngestionId;
}
/**
 * @param dataIngestionId the dataIngestionId to set
 */
public void setDataIngestionId(int dataIngestionId) {
    this.dataIngestionId = dataIngestionId;
}

/**
 * @return the stage
 */
public String getStage() {
    return stage;
}
/**
 * @param stage the stage to set
 */
public void setStage(String stage) {
    this.stage = stage;
}

}
