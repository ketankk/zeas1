package com.itc.zeas.profile.model;

import lombok.Data;

import java.util.Date;

/**
 * POJO class representing consolidated information about data source, data
 * set,schema and scheduler
 *
 * @author 19217
 */
@Data
public class Profile implements Comparable<Profile> {
    private int datasourceid;
    private String name;
    private String schedulerFrequency;
    private String sourcePath;
    private String sourceFormat;
    private String datasetName;
    private String dataSetTargetPath;
    private int datasetID;
    private String scheduler;
    private String schemaJsonBlob;
    private String schemaName;
    private String user;// schema creator
    private int schemaId;
    private int scedulerID;
    private String type;
    private String jobStatus;
    private String createdby;
    private int permissionLevel;
    private Date schemaModificationDate;


    @Override
    public int compareTo(Profile profile) {
        long t1 = this.getSchemaModificationDate().getTime();
        long t2 = profile.getSchemaModificationDate().getTime();
        if (t2 > t1)
            return 1;
        else if (t1 > t2)
            return -1;
        else
            return 0;
    }


}
