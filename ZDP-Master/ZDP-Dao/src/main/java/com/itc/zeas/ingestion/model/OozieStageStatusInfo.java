package com.itc.zeas.ingestion.model;

import lombok.Data;

@Data
public class OozieStageStatusInfo {
    private String stage;
    private String status;
    private String startTime;
    private String endTime;


}
