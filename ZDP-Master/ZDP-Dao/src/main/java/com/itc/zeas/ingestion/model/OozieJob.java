package com.itc.zeas.ingestion.model;

import lombok.Data;

import java.util.List;

/**
 * POJO class representing individual oozie job which contain information such
 * as job id, status, create time, start time and end time
 *
 * @author 19217
 */
@Data
public class OozieJob {
    private String jobId;
    List<OozieStageStatusInfo> OozieStageStatusInfoList;


}
