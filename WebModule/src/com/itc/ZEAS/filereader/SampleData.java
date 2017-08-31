/*
 * File Name: SampleData.java Description:
 * @author: 18947 Created: 26-Mar-2015 ----------------------------- -----------------------------
 */

package com.itc.zeas.filereader;

public class SampleData {

	private String fileName;
	private String fileType;
	private long fileSize;
	// it is time taken to transfer file to hdfs.
	private double timeTaken;
	// it used for test run. it is the dest/target path for hdfs
	String targetPath;

	private int noOfColumn;

	private String fixedValues;

	private String colDeli;

	private String rowDeli;

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public int getNoOfColumn() {
		return noOfColumn;
	}

	public void setNoOfColumn(int noOfColumn) {
		this.noOfColumn = noOfColumn;
	}

	public String getFixedValues() {
		return fixedValues;
	}

	public void setFixedValues(String fixedValues) {
		this.fixedValues = fixedValues;
	}

	public double getTimeTaken() {
		return timeTaken;
	}

	public void setTimeTaken(double timeTaken) {
		this.timeTaken = timeTaken;
	}

	public String getFileName() {

		return fileName;
	}

	public void setFileName(String fileName) {

		this.fileName = fileName;
	}

	public int getNoofCols() {
		return noOfColumn;
	}

	public void setNoofCols(int noOfColumn) {
		this.noOfColumn = noOfColumn;
	}

	public String getFieldlen() {
		return fixedValues;
	}

	public void setFieldlen(String fixedValues) {
		this.fixedValues = fixedValues;
	}

	public String getColDeli() {
		return colDeli;
	}

	public void setColDeli(String colDeli) {
		this.colDeli = colDeli;
	}

	public String getRowDeli() {
		return rowDeli;
	}

	public void setRowDeli(String rowDeli) {
		this.rowDeli = rowDeli;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "filename:" + fileName + " , fileType:" + fileType
				+ " , noOfColumn:" + noOfColumn + " , fixedValues:"
				+ fixedValues + " , colDeli:" + colDeli + " , rowDeli:"
				+ rowDeli + " , filesize:" + fileSize + "  ,time taken:"
				+ timeTaken + ", target path:" + targetPath;
	}

	public String getTargetPath() {
		return targetPath;
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

}
