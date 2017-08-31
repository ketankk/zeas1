package com.taphius.validation.mr;

import java.io.Serializable;
import java.text.DecimalFormat;

public class IngestionLogDetails implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8261350031015059858L;
	String ingestionFails;
	String ingestionStart;
	String ingestionComplete;
	String validationStart;
	long noOfRecords;
	long noOfCleansed;
	ValidationLogDetails validationLogDetails;
	String validationComplete;

	public long getNoOfRecords() {
		return noOfRecords;
	}

	public void setNoOfRecords(long noOfRecords) {
		this.noOfRecords = noOfRecords;
	}

	public long getNoOfCleansed() {
		return noOfCleansed;
	}

	public void setNoOfCleansed(long noOfCleansed) {
		this.noOfCleansed = noOfCleansed;
	}

	public ValidationLogDetails getValidationLogDetails() {
		return validationLogDetails;
	}

	public void setValidationLogDetails(
			ValidationLogDetails validationLogDetails) {
		this.validationLogDetails = validationLogDetails;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder logs= new StringBuilder("");
		DecimalFormat df = new DecimalFormat("###.##");
		logs.append(this.getIngestionFails()+"\n");
		logs.append(this.getIngestionStart()+"\n");
		logs.append(this.getIngestionComplete()+"\n");
		logs.append(this.getValidationStart()+"\n");
		if(this.getNoOfRecords() >0){
			logs.append(this.getNoOfRecords()+" total records ingested.\n");
			
			if(this.noOfCleansed >0) {
				logs.append(this.noOfCleansed+" cleansed records ("+df.format(this.noOfCleansed*100/this.noOfRecords)+" %).\n");
			}
			if(this.getValidationLogDetails() !=null){
				logs.append(this.getValidationLogDetails().toString());
			}
			
		}
		logs.append("\n"+this.getValidationComplete());
		return logs.toString();
	}

	public String getIngestionStart() {
		return ingestionStart;
	}

	public void setIngestionStart(String ingestionStart) {
		this.ingestionStart = ingestionStart;
	}

	public String getIngestionComplete() {
		return ingestionComplete;
	}

	public void setIngestionComplete(String ingestionComplete) {
		this.ingestionComplete = ingestionComplete;
	}

	public String getValidationStart() {
		return validationStart;
	}

	public void setValidationStart(String validationStart) {
		this.validationStart = validationStart;
	}

	public String getValidationComplete() {
		return validationComplete;
	}

	public void setValidationComplete(String validationComplete) {
		this.validationComplete = validationComplete;
	}

	public String getIngestionFails() {
		return ingestionFails;
	}

	public void setIngestionFails(String ingestionFails) {
		this.ingestionFails = ingestionFails;
	}

}
