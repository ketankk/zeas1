package com.itc.zeas.ingestion.model;

import lombok.Data;

@Data
public class OozieJobStatus {

    private String id;
    private String status;
    private String createdTime;
    private String startTime;
    private String endTime;
    private String user;
    private String appName;


}
