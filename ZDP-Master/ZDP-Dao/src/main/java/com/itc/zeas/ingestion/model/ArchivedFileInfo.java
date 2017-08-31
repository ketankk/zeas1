package com.itc.zeas.ingestion.model;

import lombok.Data;

@Data
public class ArchivedFileInfo {
    private String archivedSchemaId;
    private String archivedSchemaName;
    private int permissionLevel;


}
