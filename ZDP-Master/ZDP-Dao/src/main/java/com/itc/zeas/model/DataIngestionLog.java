package com.itc.zeas.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class DataIngestionLog {

    private int logId;
    private String batch;
    private Timestamp startTime;
    private Timestamp endTime;
    private String stage;
    private String status;
    private String jobMessage;
    private Timestamp created;
    private String createdBy;
    private Timestamp lastModified;
    private String updatedBy;
    private int dataIngestionId;


}
