/**
 *
 */
package com.itc.zeas.ingestion.model;

import lombok.Data;

/**
 * @author 17038
 */
@Data
public class ZDPScheduler {
    private int id;
    private String type;
    private String startTime;
    private String endTime;
    private int frequency;
    private int projectId;
    private String repeats;
    private String tranformType;
    private String dataset;
    private String status;


}
