package com.itc.taphius.model;

/**
 * POJO class representing individual PipelineJob job which contain information
 * such as job id and OozieJob associated with this PipelineJob.
 * 
 * @author 19217
 * 
 */
public class PipelineJob {
	private String jobId;
	private OozieJob oozieJob; 

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public OozieJob getOozieJob() {
		return oozieJob;
	}

	public void setOozieJob(OozieJob oozieJob) {
		this.oozieJob = oozieJob;
	}

	@Override
	public String toString() {
		return "pipeline jobId: " + jobId + " OozieJob Info: "
				+ oozieJob.toString();
	}
}
