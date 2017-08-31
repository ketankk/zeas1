package com.itc.zeas.v2.pipeline;

import lombok.Data;

@Data
public abstract class AbstractTransformation {


    private String id;
    private String type;
    private String inputLocation;
    private String outputLocation;
    private String datasetSchema;
    public AbstractTransformation(String type) {
        this.type = type;
    }
    public abstract String getInputArgsString();

    public abstract String getInputDatasetSchema();

    public abstract String getOutputDatasetSchema();


}
