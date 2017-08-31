package com.taphius.pipeline;

import java.util.List;

public class Stage {
    
    String Name;
    String Description;
    List<InputInfo> inputInfo;    
    List<InputInfo> outputInfo;    
    String location;    
    
    String jobType;
    String parent[];
    String child[];
    
    String to;
    String errorTo;
    
    String pigScript;
    String hiveScript;
    String sqoopScript;
    String sparkScript;
    String mapperClass;
    String reducerClass;
    String preProcess;
    String postProcess;
    
    String outputPath;
    String outSchema;
    String outputDataset;
    
    String iDataSet;
    String iDataPath;
    String iDataSchema;
    /**
     * Name of Ok-To action name upon successful execution of this stage.
     */
    String nextAction;
    
    /**
     * @return the nextAction
     */
    public String getNextAction() {
        return nextAction;
    }
    /**
     * @param nextAction the nextAction to set
     */
    public void setNextAction(String nextAction) {
        this.nextAction = nextAction;
    }
    /**
     * @return the name
     */
    public String getName() {
        return Name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        Name = name;
    }
    /**
     * @return the description
     */
    public String getDescription() {
        return Description;
    }
    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        Description = description;
    }
    /**
     * @return the inputInfo
     */
    public List<InputInfo> getInputInfo() {
        return inputInfo;
    }
    /**
     * @param inputInfo the inputInfo to set
     */
    public void setInputInfo(List<InputInfo> inputInfo) {
        this.inputInfo = inputInfo;
    }
    /**
     * @return the jobType
     */
    public String getJobType() {
        return jobType;
    }
    /**
     * @param jobType the jobType to set
     */
    public void setJobType(String jobType) {
        this.jobType = jobType;
    }
    /**
     * @return the parent
     */
    public String[] getParent() {
        return parent;
    }
    /**
     * @param parent the parent to set
     */
    public void setParent(String[] parent) {
        this.parent = parent;
    }
    /**
     * @return the child
     */
    public String[] getChild() {
        return child;
    }
    /**
     * @param child the child to set
     */
    public void setChild(String[] child) {
        this.child = child;
    }
    /**
     * @return the to
     */
    public String getTo() {
        return to;
    }
    /**
     * @param to the to to set
     */
    public void setTo(String to) {
        this.to = to;
    }
    /**
     * @return the errorTo
     */
    public String getErrorTo() {
        return errorTo;
    }
    /**
     * @param errorTo the errorTo to set
     */
    public void setErrorTo(String errorTo) {
        this.errorTo = errorTo;
    }
    /**
     * @return the pigScript
     */
    public String getPigScript() {
        return pigScript;
    }
    /**
     * @param pigScript the pigScript to set
     */
    public void setPigScript(String pigScript) {
        this.pigScript = pigScript;
    }
    /**
     * @return the hiveScript
     */
    public String getHiveScript() {
        return hiveScript;
    }
    /**
     * @param hiveScript the hiveScript to set
     */
    public void setHiveScript(String hiveScript) {
        this.hiveScript = hiveScript;
    }
    /**
     * @return the sqoopScript
     */
    public String getSqoopScript() {
        return sqoopScript;
    }
    /**
     * @param sqoopScript the sqoopScript to set
     */
    public void setSqoopScript(String sqoopScript) {
        this.sqoopScript = sqoopScript;
    }
    /**
     * @return the sparkScript
     */
    public String getSparkScript() {
        return sparkScript;
    }
    /**
     * @param sparkScript the sparkScript to set
     */
    public void setSparkScript(String sparkScript) {
        this.sparkScript = sparkScript;
    }
    /**
     * @return the mapperClass
     */
    public String getMapperClass() {
        return mapperClass;
    }
    /**
     * @param mapperClass the mapperClass to set
     */
    public void setMapperClass(String mapperClass) {
        this.mapperClass = mapperClass;
    }
    /**
     * @return the reducerClass
     */
    public String getReducerClass() {
        return reducerClass;
    }
    /**
     * @param reducerClass the reducerClass to set
     */
    public void setReducerClass(String reducerClass) {
        this.reducerClass = reducerClass;
    }
    /**
     * @return the preProcess
     */
    public String getPreProcess() {
        return preProcess;
    }
    /**
     * @param preProcess the preProcess to set
     */
    public void setPreProcess(String preProcess) {
        this.preProcess = preProcess;
    }
    /**
     * @return the postProcess
     */
    public String getPostProcess() {
        return postProcess;
    }
    /**
     * @param postProcess the postProcess to set
     */
    public void setPostProcess(String postProcess) {
        this.postProcess = postProcess;
    }
    /**
     * @return the outputPath
     */
    public String getOutputPath() {
        return outputPath;
    }
    /**
     * @param outputPath the outputPath to set
     */
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    /**
     * @return the outSchema
     */
    public String getOutSchema() {
        return outSchema;
    }
    /**
     * @param outSchema the outSchema to set
     */
    public void setOutSchema(String outSchema) {
        this.outSchema = outSchema;
    }
    /**
     * @return the outputDataset
     */
    public String getOutputDataset() {
        return outputDataset;
    }
    /**
     * @param outputDataset the outputDataset to set
     */
    public void setOutputDataset(String outputDataset) {
        this.outputDataset = outputDataset;
    }
    /**
     * @return the iDataSet
     */
    public String getiDataSet() {
        return iDataSet;
    }
    /**
     * @param iDataSet the iDataSet to set
     */
    public void setiDataSet(String iDataSet) {
        this.iDataSet = iDataSet;
    }
    /**
     * @return the iDataPath
     */
    public String getiDataPath() {
        return iDataPath;
    }
    /**
     * @param iDataPath the iDataPath to set
     */
    public void setiDataPath(String iDataPath) {
        this.iDataPath = iDataPath;
    }
    /**
     * @return the iDataSchema
     */
    public String getiDataSchema() {
        return iDataSchema;
    }
    /**
     * @param iDataSchema the iDataSchema to set
     */
    public void setiDataSchema(String iDataSchema) {
        this.iDataSchema = iDataSchema;
    }
    
    /**
     * @return the outputInfo
     */
    public List<InputInfo> getOutputInfo() {
        return outputInfo;
    }
    /**
     * @param outputInfo the outputInfo to set
     */
    public void setOutputInfo(List<InputInfo> outputInfo) {
        this.outputInfo = outputInfo;
    }
    
    /**
     * @return the location
     */
    public String getLocation() {
        return location;
    }
    /**
     * @param location the location to set
     */
    public void setLocation(String location) {
        this.location = location;
    }    
    
}
