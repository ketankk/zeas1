package com.zeas.spark.mlib.functions;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;

public class GetNumClassesFunction implements Function<String, Integer> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final int labelIndex;
	
	
	public GetNumClassesFunction( int labelIndex){

		this.labelIndex = labelIndex;
		}
	
	public Integer call(String line) {
		// logger.info("line is >>>>>>>>>>>>>>>"+line);
		String[] parts = line.split(",");
		// logger.info(parts.length);

		return Integer.parseInt(parts[labelIndex]);

	}
	
	

}
