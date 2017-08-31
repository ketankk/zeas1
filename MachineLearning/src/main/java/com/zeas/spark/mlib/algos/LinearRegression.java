package com.zeas.spark.mlib.algos;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;

import scala.Tuple2;

/**
 * This class is responsible to handle the Test and Train scenario for Linear
 * Regression Algorithm
 * 
 * @author Nisith.Nanda
 *
 */
public class LinearRegression extends AbstractMLAlgo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(LinearRegression.class);

	// JavaSparkContext sc=null;

	/*
	 * Arguments to the function should be as follow Argument 1: name of the
	 * Algorithm e,g, "linearregression" Argument 2: HDFS Path to read from such
	 * as hdfs:/user/19491/iotdata/ Argument 3: label Index such as 10 Argument
	 * 4: feature list comma separated e.g. 0,1,4,5,6,7,8,9 (start with 0)
	 * Argument 5: Location to save Model Argument 6 (Optional): Step size for
	 * linear regression e.g. stepsize=0.02 (default is 1 if not provided)
	 * Argument 7 (Optional): Number of iteration for linear regression e.g
	 * numiterations=1000 (Default is 100 if not provided) Argument 8
	 * (Optional): Delimiter (Default is ,). e.g. delimiter=, Argument 9
	 * (optional): Header (Default is false) e.g. header=true
	 * 
	 */
	/*
	 * args[0] - train/test args[1] - schema args[2] -
	 * inputPath;outputPath;linearRegression;labelIndex;FeaturesIndex;stepsize;
	 * numIterations;minBatchFraction;intercept;
	 * 
	 * @see com.zeas.spark.mlib.iface.IMLAlgorithm#train(java.lang.String[])
	 */

	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {
		final double DEFAULT_STEP_SIZE = 1.0;
		final int DEFAULT_NUM_ITERATION = 100;
		final String DEFAULT_DELIMITER = " ";
		String hdfsPath = null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		double stepSize = DEFAULT_STEP_SIZE;
		int numIterations = DEFAULT_NUM_ITERATION;
		Double minBatchFraction = null;
		boolean intercept = true;
		/*
		 * String delimiter = DEFAULT_DELIMITER; boolean hasHeader=true;
		 */
		ArrayList<Integer> featureList = new ArrayList();

		if (args.length < 3) {
			handleException("Linear Regression: Insufficient number of arguments. expected 3 got " + args.length);
			return;
		}
		try {
			String[] params = args[2].split(";");
			if (params.length < 9) {
				handleException("Linear Regression: Insufsficient number of arguments for the third argument. \n arg = "
						+ params.length);
				return;

			}
			// String [] params2 =args[3].split(";");
			hdfsPath = params[0];
			labelIndex = convertToInt(params[3]);
			String[] featureStrList = params[4].split(",");
			for (int i = 0; i < featureStrList.length; i++) {
				featureList.add(convertToInt(featureStrList[i]));
			}
			modelSaveLocation = params[1];
			stepSize = convertToDouble(params[5]);
			numIterations = convertToInt(params[6]);
			minBatchFraction = new Double(params[7]);
			intercept = new Boolean(params[8]);

		} catch (Exception e) {
			handleException("Linear Regression: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("LinearRegressionTraining");
		// GetLabeledData gl =new GetLabeledData();

		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		// JavaRDD<LabeledPoint> trainingDataLabeled =
		// getLabeledData(trainingData, labelIndex, featureList, delimiter);
		trainingDataLabeled.cache();

		// Building the model
		// final LinearRegressionModel model =
		// LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingDataLabeled),
		// numIterations, stepSize);
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(intercept);
		alg.optimizer().setMiniBatchFraction(minBatchFraction);
		alg.optimizer().setNumIterations(numIterations);
		alg.optimizer().setStepSize(stepSize);
		LinearRegressionModel model = alg.run(trainingDataLabeled.rdd());
		saveModel(model, modelSaveLocation, sc);
		logger.info("Model Weights : " + model.weights());
		logger.info("Model Intercept : " + model.intercept());
	}

	// saving the model...
	public String saveModel(LinearRegressionModel model, String modelSaveLocation, JavaSparkContext sc) {
		model.save(sc.sc(), modelSaveLocation);
		printMessage("Model saved at " + modelSaveLocation);
		return modelSaveLocation;

	}

	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String[] args) {
		/*
		 * args[0] - test args[1] - schema args[2] -
		 * inputpath;output;linear_regression;savedModel;labelIndex;
		 * FeaturesIndex;
		 * 
		 * @see com.zeas.spark.mlib.iface.IMLAlgorithm#train(java.lang.String[])
		 */
		JavaSparkContext sc = getSparkContext("LinearRegressionTesting");
		String[] params = args[2].split(";");
		String hdfsPath = params[0];
		String modelSavedLocation = params[3];
		String outputPath = params[1];
		int labelIndex = convertToInt(params[4]);
		ArrayList<Integer> featureList = new ArrayList();
		String[] featureStrList = params[5].split(",");
		for (int i = 0; i < featureStrList.length; i++) {
			featureList.add(convertToInt(featureStrList[i]));
		}
		JavaRDD<String> testData = sc.textFile(hdfsPath);
		// testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));
		// getting the details of saved model
		final LinearRegressionModel savedModel = LinearRegressionModel.load(sc.sc(), modelSavedLocation);
		// creating RDD to get detiails- label,features,predictions
		JavaRDD<String> results = testingDataLabeled.map(new Function<LabeledPoint, String>() {
			public String call(LabeledPoint point) {
				double prediction = savedModel.predict(point.features());
				return Double.toString(point.label()) + " , " + point.features().toString() + " , "
						+ Double.toString(prediction);
			}
		});
		// saving results to HDFS file
		results.saveAsTextFile(outputPath);
		// evaluation metrics
		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");
		String sumModel = null;

		logger.info("Results stored in file : " + params[1]);
		testingDataLabeled.cache();

		JavaRDD<Tuple2<Object, Object>> valueAndPred = testingDataLabeled
				.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						double prediction = savedModel.predict(point.features());
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				});

		Vector weights = savedModel.weights();
		logger.info("Weights for features:  " + weights);
		tIF.append("Weights for features: ");
		for (int i = 0; i < weights.toArray().length; i++) {
			if ((i + 1) == weights.toArray().length) {
				tIF.append(MlibCommon.getFormatedDoubleValue(weights.toArray()[i])).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(weights.toArray()[i])).append(MlibCommon.DATA_DELIM);
			}
		}
		sumModel = savedModel.toString();
		logger.info("Model Summary: " + sumModel);
		tIF.append("Model Summary: ").append(sumModel).append("\n");
		tIF.append("Intercept : ").append(MlibCommon.getFormatedDoubleValue(savedModel.intercept())).append("\n");

		// Evaluation Metrics (Regression)
		RegressionMetrics metrics = new RegressionMetrics(valueAndPred.rdd());
		// Squared error
		logger.info("MSE: " + metrics.meanSquaredError() + "\n");
		logger.info("RMSE: " + metrics.rootMeanSquaredError() + "\n");
		// R-squared
		logger.info("R Squared: " + metrics.r2() + "\n");

		// Mean absolute error
		logger.info("MAE: " + metrics.meanAbsoluteError() + "\n");

		// Explained variance
		logger.info("Explained Variance: " + metrics.explainedVariance() + "\n");

		/* Write the Evaluation Metrics to a txt file */
		tIF.append("Type: ").append(MlibCommon.REGRESSION).append("\n");
		tIF.append("Algorithm: ").append(params[2]).append("\n");
		tIF.append("MSE: ").append(MlibCommon.getFormatedDoubleValue(metrics.meanSquaredError())).append("\n");
		tIF.append("RMSE: ").append(MlibCommon.getFormatedDoubleValue(metrics.rootMeanSquaredError())).append("\n");
		tIF.append("R Squared: ").append(MlibCommon.getFormatedDoubleValue(metrics.r2())).append("\n");
		tIF.append("MAE: ").append(MlibCommon.getFormatedDoubleValue(metrics.meanAbsoluteError())).append("\n");
		tIF.append("Explained Variance: ").append(MlibCommon.getFormatedDoubleValue(metrics.explainedVariance()))
				.append("\n");
		logger.info("Evaluation Metrics stored in " + evalPath);
		Path newFilePath = new Path(evalPath);
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
			hdfs.createNewFile(newFilePath);
			FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
			fsOutStream.write(tIF.toString().getBytes());
			fsOutStream.close();
		} catch (Exception e) {
			logger.error("LinearRegression: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}

}
