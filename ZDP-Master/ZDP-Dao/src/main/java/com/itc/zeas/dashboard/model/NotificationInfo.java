package com.itc.zeas.dashboard.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class NotificationInfo implements Comparable<NotificationInfo> {

    private Long id;
    private String status_message;
    private Timestamp time_occured;
    private String component_type;
    private String operation_type;
    private String timeInfo;
    private Long component_id;


    @Override
    public int compareTo(NotificationInfo o) {

        return o.time_occured.compareTo(this.time_occured);
    }


}
