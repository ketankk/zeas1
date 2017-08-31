package com.taphius.pipeline;
/**
 * Defines the POJO for Pipeline /Workflow
 * @author 16795
 *
 */
public class Pipeline {
    int id;	
    String Name;
	String Description;
	String frequency;
	String offset;
	String startStage;
	String endStage;
	
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
	 * @return the frequency
	 */
	public String getFrequency() {
		return frequency;
	}
	/**
	 * @param frequency the frequency to set
	 */
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	/**
	 * @return the offset
	 */
	public String getOffset() {
		return offset;
	}
	/**
	 * @param offset the offset to set
	 */
	public void setOffset(String offset) {
		this.offset = offset;
	}
	/**
	 * @return the startStage
	 */
	public String getStartStage() {
		return startStage;
	}
	/**
	 * @param startStage the startStage to set
	 */
	public void setStartStage(String startStage) {
		this.startStage = startStage;
	}
	/**
	 * @return the endStage
	 */
	public String getEndStage() {
		return endStage;
	}
	/**
	 * @param endStage the endStage to set
	 */
	public void setEndStage(String endStage) {
		this.endStage = endStage;
	}
	
}
