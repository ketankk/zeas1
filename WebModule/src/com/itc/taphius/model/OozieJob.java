package com.itc.taphius.model;

import java.util.List;

/**
 * POJO class representing individual oozie job which contain information such
 * as job id, status, create time, start time and end time
 * 
 * @author 19217
 * 
 */
public class OozieJob {
	private String jobId;
	List<OozieStageStatusInfo> OozieStageStatusInfoList;
	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public List<OozieStageStatusInfo> getOozieStageStatusInfoList() {
		return OozieStageStatusInfoList;
	}
	public void setOozieStageStatusInfoList(
			List<OozieStageStatusInfo> oozieStageStatusInfoList) {
		OozieStageStatusInfoList = oozieStageStatusInfoList;
	}
	
	
	
	
	
	
	
	
	// private String status;
	// private String startTime;
	// private String crateTime;
	// private String endTime;
	//
	// public String getJobId() {
	// return jobId;
	// }
	//
	// public void setJobId(String jobId) {
	// this.jobId = jobId;
	// }
	//
	// public String getStatus() {
	// return status;
	// }
	//
	// public void setStatus(String status) {
	// this.status = status;
	// }
	//
	// public String getStartTime() {
	// return startTime;
	// }
	//
	// public void setStartTime(String startTime) {
	// this.startTime = startTime;
	// }
	//
	// public String getCrateTime() {
	// return crateTime;
	// }
	//
	// public void setCrateTime(String crateTime) {
	// this.crateTime = crateTime;
	// }
	//
	// public String getEndTime() {
	// return endTime;
	// }
	//
	// public void setEndTime(String endTime) {
	// this.endTime = endTime;
	// }
	//
	// @Override
	// public String toString() {
	// return "jobId: " + jobId + " status: " + status + " crateTime: "
	// + crateTime + "startTime: " + startTime + "endTime: " + endTime;
	// }
}
