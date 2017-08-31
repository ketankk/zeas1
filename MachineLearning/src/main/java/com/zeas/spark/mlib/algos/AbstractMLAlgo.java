package com.zeas.spark.mlib.algos;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.zeas.spark.mlib.iface.IMLAlgorithm;

public abstract class AbstractMLAlgo implements IMLAlgorithm,Serializable {
	
	public static final long serialVersionUID = 1L;
	public static Logger logger = Logger.getLogger(AbstractMLAlgo.class);
	protected JavaSparkContext getSparkContext(String appName){
		SparkConf conf = new SparkConf().setAppName(appName);
		return new JavaSparkContext(conf);
	}
	
	protected void handleException(String errorMessage){
		System.out.println(errorMessage);
		logger.error(errorMessage);
	}
	
	protected int convertToInt(String str){
		 return Integer.parseInt(str);
	}
	
	protected double convertToDouble(String str){
		 return Double.parseDouble(str);
	}
	
	protected void printMessage(String str){
		System.out.println(str);
	}
}
