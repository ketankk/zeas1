package com.itc.zeas.v2.pipeline;
/**
 * This class represents the Stage.
 * A stage can be Hive|Pig|MR job 
 * It captures input/output dataset for the stage and other info like Query
 * or mapper/reducer class for MR job.
 * @author 16795
 *
 */
public class Stage {
   
	/**
     * Stage Name
     */
    private String name;
    /**
     * Unique identifier for the Stage
     */
    private int id;
    /**
     * Type of the stage - Hive|Pig|MR
     */
    private String stageType;
    /**
     * Input/s to the Stage - dataset
     * Stage can have multiple inputs hence Array.
     */
   // private String inputDataset[];
    
    /**
     * Input/s to the Stage - actual HDFS path     * 
     */
    private String inputPath;
    /**
     * Output dataset of the stage
     */
    private String outputDataset;
    
    /**
     * HDFS path of output dataset.
     */
    private String outputPath;
    /**
     * Schema for the output dataset.
     */
    private String outputSchema;
    /**
     * List of parents for this Stage, in pipeline graph.
     * 
     */
    private String parent[];
    /**
     * List of children for this stage, in pipeline graph.
     */
    private String child[];
    
    private String ok_to; //if parents exist assign joining tag
    /**
     * Original parent list, contains both dataets and transformations if any.
     */
    private String input[];
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
    private String join;// join the next job
    /**
     * For Pig Stage - pig script to be executed
     */
    private String pigScript;
    /**
     * For Hive Stage - HQL query to be executed
     */
    private String hiveScript;
    /**
     * For MR Stage - mapper class name 
     */
    private String mapperClass;
    /**
     * For MR Stage - reducer class name
     */
    private String reducerClass;
    /**
     * For MR Stage - output key class name 
     */
    private String outKeyClassName;
    /**
     * For MR Stage - output value class name
     */
    private String outValueClassName;
    /**
     * Local FS location where uploaded jar is available.
     */
    private String mapRedJarPath;
    /**
     * pre-process activity to be done before executing action
     */
    private String preProcess;
    /**
     * post-process activity after action completion.
     */
    private String postProcess;
    /**
     * @return the hiveScript
     */
    public String getHiveScript() {
        return hiveScript;
    }
    public String getOutKeyClassName() {
		return outKeyClassName;
	}
	public void setOutKeyClassName(String outKeyClassName) {
		this.outKeyClassName = outKeyClassName;
	}
	public String getOutValueClassName() {
		return outValueClassName;
	}
	public void setOutValueClassName(String outValueClassName) {
		this.outValueClassName = outValueClassName;
	}
	/**
     * @param hiveScript the hiveScript to set
     */
    public void setHiveScript(String hiveScript) {
        this.hiveScript = hiveScript;
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
     * @return the ok_to
     */
    public String getOk_to() {
        return ok_to;
    }
    /**
     * @param ok_to the ok_to to set
     */
    public void setOk_to(String ok_to) {
    	 String ok = ok_to.equalsIgnoreCase("end")? ok_to:"m_"+ok_to;
        this.ok_to = ok;
    }
    /**
     * @return the join
     */
    public String getJoin() {
        return join;
    }
    /**
     * @param join the join to set
     */
    public void setJoin(String join) {
        this.join = join;
    }
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }
    /**
     * @return the stageType
     */
    public String getStageType() {
        return stageType;
    }
    /**
     * @param stageType the stageType to set
     */
    public void setStageType(String stageType) {
        this.stageType = stageType;
    }
    /**
     * @return the inputDataset
     */
   /* public String[] getInputDataset() {
        return inputDataset;
    }
    *//**
     * @param inputDataset the inputDataset to set
     *//*
    public void setInputDataset(String[] inputDataset) {
        this.inputDataset = inputDataset;
    }*/
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
    public String[] getParent() {
        return parent;
    }
    public void setParent(String parent[]) {
        this.parent = parent;
    }
    public String[] getChild() {
        return child;
    }
    public void setChild(String child[]) {
        if(child[0].equalsIgnoreCase(""))
            this.child = new String[0];
        else 
            this.child = child;
    }
    public String getOutputPath() {
        return outputPath;
    }
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    public String getOutputSchema() {
        return outputSchema;
    }
    public void setOutputSchema(String outputSchema) {
        this.outputSchema = outputSchema;
    }
    public String getInputPath() {
        return inputPath;
    }
    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }
    public String getMapRedJarPath() {
        return mapRedJarPath;
    }
    public void setMapRedJarPath(String mapRedJarPath) {
        this.mapRedJarPath = mapRedJarPath;
    }
    
    //added for new project implementation
    
    AbstractTransformation absTransformation;
    
    public AbstractTransformation getAbsTransformation() {
		return absTransformation;
	}
	public void setAbsTransformation(AbstractTransformation absTransformation) {
		this.absTransformation = absTransformation;
	}
	public String[] getInput() {
		return input;
	}
	public void setInput(String input[]) {
		this.input = input;
	}

}
