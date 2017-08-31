package com.itc.zeas.profile.model;

import lombok.Data;

@Data
public class SampleData {

	private String fileName;
	private String fileType;
	private long fileSize;
	// it is time taken to transfer file to hdfs.
	private double timeTaken;
	// it used for test run. it is the dest/target path for hdfs
	private String targetPath;
	private int noOfColumn;
	private String fixedValues;
	private String colDeli;
	private String rowDeli;

}
