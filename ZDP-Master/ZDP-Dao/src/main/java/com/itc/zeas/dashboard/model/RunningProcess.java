package com.itc.zeas.dashboard.model;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author Ketan on 4/29/2017.
 *//*
 * This is private class to make the objects stor the details
 *  of running process type used in method getRunningProcesses
 */
@Data
public class RunningProcess implements Comparable<RunningProcess> {
    private String name;
    private String startedBy;
    private Timestamp startedOn;
    private String status;
    private String jobId;

    @Override
    public int compareTo(RunningProcess o) {
        return this.startedOn.compareTo(o.startedOn);
    }


}
