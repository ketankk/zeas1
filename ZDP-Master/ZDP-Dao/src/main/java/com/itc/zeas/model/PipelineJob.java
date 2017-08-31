package com.itc.zeas.model;

import com.itc.zeas.ingestion.model.OozieJob;
import lombok.Data;

/**
 * POJO class representing individual PipelineJob job which contain information
 * such as job id and OozieJob associated with this PipelineJob.
 * 
 * @author 19217
 * 
 */
@Data
public class PipelineJob {
	private String jobId;
	private OozieJob oozieJob;
}
