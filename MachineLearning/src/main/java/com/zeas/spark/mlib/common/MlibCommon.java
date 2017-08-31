package com.zeas.spark.mlib.common;

/**
 * This class is responsible to maintain all the common properties and methods.
 * 
 * @author Nisith.Nanda
 *
 */
public class MlibCommon {

	public static final String BINARY_CLASSIFICATION_METRICS = "Type,Algorithm,Area under ROC curve,Number of Bins,Precision by threshold,Recall by threshold,F2 Score by threshold,Precision-recall curve,ROC curve,Area under precision-recall curve";
	public static final String REGRESSION_METRICS = "Type,Algorithm,Weights for features,Model Summary,LinearRegressionModel,intercept, numFeatures,MSE,RMSE,R Squared,MAE,Explained Variance";
	public static final String MULTI_CLASSIFICATION_METRICS = "Type,Algorithm,Precision,Recall,F1 Score,Weighted Precision,Weighted Recall,Weighted F1 score,Weighted False Positive Rate,Num Of Labels";
	public static final String CLUSTERING_METRICS = "Type,Algorithm,Cluster Centers,Number of Clusters";
	public static final String BINARY_CLASSIFICATION = "binary classification";
	public static final String REGRESSION = "regression";
	public static final String MULTI_CLASSIFICATION = "multiple classification";
	public static final String CLUSTERING = "clustering";
	public static final String FILE_EXTN = "_eval.txt";
	public static final String DATA_DELIM = "-";
	public static final double PRECISION = 10000.0;

	/**
	 * This method is used to format the double values to 4 digits after decimal
	 * point
	 * 
	 * @param val
	 *            The input double value
	 * @return return the formated double value
	 */
	public static double getFormatedDoubleValue(double val) {

		if (Double.isNaN(val) || Double.isInfinite(val)) {
			return val;
		} else {
			val = (Math.round(val * MlibCommon.PRECISION) / MlibCommon.PRECISION);
			return val;
		}

	}
}
