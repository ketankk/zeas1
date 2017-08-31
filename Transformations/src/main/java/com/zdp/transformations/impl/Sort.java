package com.zdp.transformations.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * sort according to the column index, which sent as parameter.
 * @author 19173
 *
 */

public class Sort extends AbstractTransformations implements Serializable {
	

	private static final long serialVersionUID = 1L;
	public String[] args;
	public Sort(String[] arg) {
		args = arg;
	}
	
	public void execute(){
		
		//args[0] - type
		//args[1] - schema
		//args[2] - inputPath1; outputPath;sortIndex;sortIndexDatatype
		
		String [] params = args[2].split(";");
		
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> inputDataRDD = sc.textFile(params[0]);
		final int sortIndex = Integer.parseInt(params[2]);
		final String sortIndexDatatype = params[3];
		
		
		if(sortIndexDatatype.equalsIgnoreCase("string")){
		    JavaPairRDD<String,String> keyValuePair = inputDataRDD.mapToPair(new PairFunction<String,String,String>(){
			
			   public Tuple2<String,String> call(String line) {
				String[] parts = line.split(",");
				String temp = "";
				String key = parts[sortIndex];
				
				for (int i = 0; i < parts.length; i++) {
					temp = temp + parts[i] + ",";
				}
				 return new Tuple2<String ,String>(key,temp.substring(0,temp.length()-1));
				//return temp.substring(0,temp.length()-1);		
			}
		});
		    
		    JavaPairRDD<String, String> sortedPair = keyValuePair.sortByKey();
		    JavaRDD<String> result= sortedPair.map(new Function<Tuple2<String,String>,String>(){
		    	public String call(Tuple2<String,String> s){
		    		return s._2;
		    	}
		    	});
		    result.saveAsTextFile(params[1]);
		
		}
		else{
			
			JavaPairRDD<Double,String> keyValuePair = inputDataRDD.mapToPair(new PairFunction<String,Double,String>(){
				
				   public Tuple2<Double,String> call(String line) {
					String[] parts = line.split(",");
					String temp = "";
					Double key = Double.parseDouble(parts[sortIndex]);
					
					for (int i = 0; i < parts.length; i++) {
						temp = temp + parts[i] + ",";
					}
					 return new Tuple2<Double ,String>(key,temp.substring(0,temp.length()-1));
					//return temp.substring(0,temp.length()-1);		
				}
			});
			
			 JavaPairRDD<Double, String> sortedPair = keyValuePair.sortByKey();
			    JavaRDD<String> result= sortedPair.map(new Function<Tuple2<Double,String>,String>(){
			    	public String call(Tuple2<Double,String> s){
			    		return s._2;
			    	}
			    	});
			    result.saveAsTextFile(params[1]);
			
		}
		
		
		
	}

}
