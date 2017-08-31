package com.zeas.spark.mlib.factory;

import org.apache.log4j.Logger;

import com.zeas.spark.mlib.algos.BinaryLogisticRegression;
import com.zeas.spark.mlib.algos.DecisionTreeClassification;
import com.zeas.spark.mlib.algos.DecisionTreeRegression;
import com.zeas.spark.mlib.algos.GradientBoostedClassification;
import com.zeas.spark.mlib.algos.KMeansClustering;
import com.zeas.spark.mlib.algos.LinearRegression;
import com.zeas.spark.mlib.algos.MultiClassLogisticRegression;
import com.zeas.spark.mlib.algos.NaiveBayesClassification;
import com.zeas.spark.mlib.algos.RandomForestClassification;
import com.zeas.spark.mlib.algos.RandomForestRegression;
import com.zeas.spark.mlib.algos.SVMClassification;
import com.zeas.spark.mlib.iface.IMLAlgorithm;

public class MLAlgoFactory {
	private static Logger logger = Logger.getLogger(MLAlgoFactory.class);

	/*
	 * public static void main(String args[]){ if(args==null || args.length<1){
	 * System.err.println("The first argument should be the name of algorithm");
	 * return; } IMLAlgorithm algo = createAlgo(args); if(algo==null){
	 * System.err.println("Specify the name of algo correctly"); return; }
	 * algo.train(args); }
	 */
	public static IMLAlgorithm createAlgo(String[] args) {
		logger.info(args);
		if (args[2].split(";")[2].equalsIgnoreCase("Linear_Regression")) {
			return new LinearRegression();
		} else if (args[2].split(";")[2].equalsIgnoreCase("Binary_Logistic_Regression")) {
			return new BinaryLogisticRegression();
		} else if (args[2].split(";")[2].equalsIgnoreCase("Multiclass_Logistic_Regression")) {
			return new MultiClassLogisticRegression();
		} else if (args[2].split(";")[2].equalsIgnoreCase("Kmeans_Clustering")) {
			return new KMeansClustering();
		}
		
		else if(args[2].split(";")[2].equalsIgnoreCase("Random_forest_Classification")){
			return new RandomForestClassification();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("Random_forest_Regression")){
			return new RandomForestRegression();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("Decision_Tree_Regression")){
			return new DecisionTreeRegression();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("Decision_Tree_Classification")){
			return new DecisionTreeClassification();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("Gradient_Boosted_Classification")){
			return new GradientBoostedClassification();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("Naive_Bayes_Classification")){
			return new NaiveBayesClassification();
		}
		else if(args[2].split(";")[2].equalsIgnoreCase("SVM_Classification")){
			return new SVMClassification();
		}
		
		


		return null;
	}

}
