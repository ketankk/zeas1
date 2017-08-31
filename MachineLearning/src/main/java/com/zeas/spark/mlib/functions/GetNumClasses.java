package com.zeas.spark.mlib.functions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.zeas.spark.mlib.algos.MultiClassLogisticRegression;


public class GetNumClasses  {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(MultiClassLogisticRegression.class);
	JavaRDD<String> trainingData;
	int labelIndex ;
	public GetNumClasses(JavaRDD<String> trainingData,int labelIndex ){
		this.trainingData =trainingData;
		this.labelIndex = labelIndex;
	}

	public int numClasses() {
		final int label = labelIndex;
		 logger.info("label is >>>>>>>>>>>>>>>>>>>>"+label);
		JavaRDD<Integer> labelData = trainingData
				.map(new GetNumClassesFunction(labelIndex));

		JavaRDD<Integer> distinctValues = labelData.distinct();
		List<Integer> list = distinctValues.collect();
		int maxValue = 0;
		Iterator<Integer> iter = list.iterator();
		int currentVal;
		while (iter.hasNext()) {
             currentVal =iter.next();
			if (currentVal >= maxValue) {
				maxValue = currentVal;
			}

		}
		int numClasses = maxValue + 1;
		 logger.info("numClasses >>>>>>>>>>>>>>>>>>>>"+numClasses);
		return numClasses;

	}
}
