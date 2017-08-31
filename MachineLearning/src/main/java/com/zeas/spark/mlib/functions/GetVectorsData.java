package com.zeas.spark.mlib.functions;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

public class GetVectorsData implements Function<String,Vector> {
	
	
	private static final long serialVersionUID = 1L;
	final ArrayList<Integer> featureIndices ;

	
public GetVectorsData(final ArrayList<Integer> featureIndices){
      this.featureIndices = featureIndices;
	
    }

    public Vector call(String line) {
		String[] parts = line.split(",");
		double[] v = new double[featureIndices.size()];
		for (int i = 0; i < featureIndices.size(); i++){
			v[i] = Double.parseDouble(parts[featureIndices.get(i)]);
		}
		return Vectors.dense(v);
		
	
	
	}


}
