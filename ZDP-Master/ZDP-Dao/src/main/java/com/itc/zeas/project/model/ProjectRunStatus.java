package com.itc.zeas.project.model;

import com.itc.zeas.project.model.ModuleRunStatus;
import lombok.Data;

import java.util.Date;
import java.util.List;


/**
 * Model class for holding Project status
 */
@Data
public class ProjectRunStatus {
    private Long projectId;
    private Integer version;
    private String runStatus;
    private Date startTime;
    private Date endTime;
    private List<ModuleRunStatus> moduleRunStatusList;


}
