package com.itc.zeas.model;

import lombok.Data;

@Data
public class DataSourcer {
    private int id;
    private int userId;
    private String jsonblob;
    private String name;
    private String type;
    private DataDisctionaryEntity dataDisctionaryEntity;
    private String location;
    private String format;
    private boolean isActive;
    private String createdBy;
    private String lastUpdated;
    private String dataSourceId;
    private String schemaType;
    private String frequency;

}
