package com.zeas.spark.mlib.functions;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.log4j.Logger;

import com.zeas.spark.mlib.algos.BinaryLogisticRegression;
import com.zeas.spark.mlib.algos.RandomForestClassification;

public class GetLabeledData implements Function<String, LabeledPoint> {
	
	private static Logger logger=Logger.getLogger(RandomForestClassification.class);
	
	
	private static final long serialVersionUID = 1L;
	final int labelIndex;
	final ArrayList<Integer> featureIndices;
	


	
public GetLabeledData(final int labelIndex, final ArrayList<Integer> featureIndices){

	
	this.labelIndex = labelIndex;
	this.featureIndices = featureIndices;
	
}


    	
	public LabeledPoint call(String line) {
		String[] parts = line.split(",");
		String label =parts[labelIndex];
		double[] v = new double[featureIndices.size()];
		for (int i = 0; i < featureIndices.size(); i++){
			v[i] = Double.parseDouble(parts[featureIndices.get(i)]);
		}
		return new LabeledPoint(Double.parseDouble(label), Vectors.dense(v));

	}


}
