package com.zeas.spark.mlib;

import com.zeas.spark.mlib.factory.MLAlgoFactory;
import com.zeas.spark.mlib.iface.IMLAlgorithm;

/**
 * Factory pattern to call algorithms
 */
public class RunMLAlgorithm {
	public static void main(String[] args) {
		if (args[0].equalsIgnoreCase("Compare_Model")) {
			CompareModel compareModel = new CompareModel();
			compareModel.createComparisonFile(args);
		} else {
			MLAlgoFactory tf = new MLAlgoFactory();
			IMLAlgorithm lr = tf.createAlgo(args);
			if (args[0].equalsIgnoreCase("train")) {
				lr.train(args);
			} else if (args[0].equalsIgnoreCase("test")) {
				lr.test(args);
			}
		}
	}

}