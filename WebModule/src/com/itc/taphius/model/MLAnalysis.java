package com.itc.taphius.model;

public class MLAnalysis {
	
	int mlId;
	ProcessedPipeline training;
	ProcessedPipeline testing;
	String algorithm;
	int accuracy;
	
	/**
	 * @return the mlId
	 */
	public int getMlId() {
		return mlId;
	}
	/**
	 * @param mlId the mlId to set
	 */
	public void setMlId(int mlId) {
		this.mlId = mlId;
	}
	/**
	 * @return the training
	 */
	public ProcessedPipeline getTraining() {
		return training;
	}
	/**
	 * @param training the training to set
	 */
	public void setTraining(ProcessedPipeline training) {
		this.training = training;
	}
	/**
	 * @return the testing
	 */
	public ProcessedPipeline getTesting() {
		return testing;
	}
	/**
	 * @param testing the testing to set
	 */
	public void setTesting(ProcessedPipeline testing) {
		this.testing = testing;
	}
	/**
	 * @return the algorithm
	 */
	public String getAlgorithm() {
		return algorithm;
	}
	/**
	 * @param algorithm the algorithm to set
	 */
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	/**
	 * @return the accuracy
	 */
	public int getAccuracy() {
		return accuracy;
	}
	/**
	 * @param accuracy the accuracy to set
	 */
	public void setAccuracy(int accuracy) {
		this.accuracy = accuracy;
	}
	

}
