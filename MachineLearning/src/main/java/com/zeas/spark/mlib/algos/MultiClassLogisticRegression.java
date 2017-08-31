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
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;
import com.zeas.spark.mlib.functions.GetNumClasses;

import scala.Tuple2;

/**
 * This class is responsible to handle the Test and Train scenario for
 * MultiClass Logistic Regression Algorithm
 * 
 * @author Nisith.Nanda
 *
 */
public class MultiClassLogisticRegression extends AbstractMLAlgo implements Serializable {

	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(MultiClassLogisticRegression.class);

	/*
	 * Arguments to the function should be as follow Argument 1: name of the
	 * Algorithm e,g, "logisticregression" Argument 2: HDFS Path to read from
	 * such as hdfs:/user/19491/iotdata/ Argument 3: label Index such as 10
	 * Argument 4: feature list comma separated e.g. 0,1,4,5,6,7,8,9 (start with
	 * 0) Argument 5: Location to save Model Argument 6 (Optional): Number of
	 * Classes for logistic regression e.g numiterations=10 (Default is 100 if
	 * not provided) Argument 7 (Optional): Delimiter (Default is ,). e.g.
	 * delimiter=, Argument 8 (optional): Header (Default is false) e.g.
	 * header=true
	 */

	/*
	 * args[0] - LinearRegression args[1] - train/test args[2] -
	 * inputpath;outputPath;multiclass_logistic_regression;labelIndex;
	 * FeaturesIndex;numOfIterations;regParam;intercept;
	 *
	 */
	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {

		final int DEFAULT_NUM_CLASSES = 10;
		// final String DEFAULT_DELIMITER =" ";
		String hdfsPath = null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		int numClasses = DEFAULT_NUM_CLASSES;
		Integer numOfIterations = 100;
		Double regParam = 0.1;
		boolean intercept = true;
		// String delimiter = DEFAULT_DELIMITER;
		// boolean hasHeader=true;
		ArrayList<Integer> featureList = new ArrayList<Integer>();

		if (args.length < 3) {
			handleException(
					"Multi Logistic Regression: Insufficient number of arguments. Expected 3 but got " + args.length);
			return;
		}
		try {
			String[] params = args[2].split(";");
			if (params.length < 8) {
				handleException(
						"Multi Logistic Regression: Insufficient number of arguments for the third argument. \n arg = "
								+ args[2]);
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
			if (params.length > 5) {
				numOfIterations = new Integer(params[5]);
				regParam = new Double(params[6]);
				intercept = new Boolean(params[7]);
			}
			/*
			 * if(params.length>5){
			 * 
			 * numClasses=convertToInt(params[5]);
			 * 
			 * }
			 */

		} catch (Exception e) {
			handleException("Multi logistic Regression: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("MultiLogisticRegressionTraining");
		// get the number of classes
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		GetNumClasses getNumClasses = new GetNumClasses(trainingData, labelIndex);
		numClasses = getNumClasses.numClasses();
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		// JavaRDD<LabeledPoint> trainingDataLabeled =
		// getLabeledData(trainingData, labelIndex, featureList, delimiter);

		trainingDataLabeled.cache();

		// Building the model
		// final LogisticRegressionModel model = new
		// LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(trainingDataLabeled.rdd());
		LogisticRegressionWithLBFGS alg = new LogisticRegressionWithLBFGS();
		alg.optimizer().setNumIterations(numOfIterations);
		alg.optimizer().setRegParam(regParam);
		alg.setNumClasses(numClasses);
		alg.setIntercept(intercept);
		final LogisticRegressionModel model = alg.run(trainingDataLabeled.rdd());
		saveModel(model, modelSaveLocation, sc);

		logger.info("Model Weights: " + model.weights());
		logger.info("Model Intercept : " + model.intercept());

	}

	// saving the model...
	private void saveModel(LogisticRegressionModel model, String modelSaveLocation, JavaSparkContext sc) {
		model.save(sc.sc(), modelSaveLocation);
		printMessage("Model saved at " + modelSaveLocation);
	}

	/*
	 * args[0] - test args[1] - schema args[2] -
	 * inputpath;output;MultiClass_Logistic_Regression;savedModellocation;
	 * labelIndex;FeaturesIndex;
	 */
	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String[] args) {

		JavaSparkContext sc = getSparkContext("MultiLogisticRegressionTesting");
		String[] params = args[2].split(";");
		String outputPath = params[1];
		String hdfsPath = params[0];
		String modelSavedLocation = params[3];
		int labelIndex = convertToInt(params[4]);
		ArrayList<Integer> featureList = new ArrayList();
		String[] featureStrList = params[5].split(",");
		for (int i = 0; i < featureStrList.length; i++) {
			featureList.add(convertToInt(featureStrList[i]));
		}

		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");
		String sumModel = "";
		int nClasses = 0;

		JavaRDD<String> testData = sc.textFile(hdfsPath);
		// testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));
		// getting the details of saved model
		final LogisticRegressionModel savedModel = LogisticRegressionModel.load(sc.sc(), modelSavedLocation);
		// creating RDD to get details- label,features,predictions
		JavaRDD<String> results = testingDataLabeled.map(new Function<LabeledPoint, String>() {
			public String call(LabeledPoint point) {
				double prediction = savedModel.predict(point.features());
				return Double.toString(point.label()) + " , " + point.features().toString() + " , "
						+ Double.toString(prediction);
			}
		});
		// saving results to HDFS file
		results.saveAsTextFile(outputPath);
		logger.info("Results stored in file: " + outputPath);

		/* Details of the Model */
		Vector weights = savedModel.weights();
		logger.info("Weights for features:\n " + weights);

		tIF.append("Weights for features: ");
		for (int i = 0; i < weights.toArray().length; i++) {
			if ((i + 1) == weights.toArray().length) {
				tIF.append(MlibCommon.getFormatedDoubleValue(weights.toArray()[i])).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(weights.toArray()[i])).append(",");
			}
		}
		// double intercept = savedModel.intercept();
		logger.info("Intercept: " + MlibCommon.getFormatedDoubleValue(savedModel.intercept()));
		tIF.append("Intercept: ").append(MlibCommon.getFormatedDoubleValue(savedModel.intercept())).append("\n");
		sumModel = savedModel.toString();
		logger.info("Model Summary: " + sumModel);
		tIF.append("Model Summary: ").append(sumModel);
		nClasses = savedModel.numClasses();
		logger.info("Number of Classes: " + nClasses);
		tIF.append("Number of Classes: ").append(nClasses).append("\n");
		// Summary of model in String

		JavaRDD<Tuple2<Object, Object>> valueAndPred = testingDataLabeled
				.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						double prediction = savedModel.predict(point.features());
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				});
		// Evaluation Metrics (Multiple Classes Classification)
		MulticlassMetrics metrics = new MulticlassMetrics(valueAndPred.rdd());

		// Confusion matrix
		Matrix confusion = metrics.confusionMatrix();
		logger.info("Confusion matrix: \n" + confusion.toString());

		// Overall statistics
		logger.info("Precision: " + metrics.precision());
		logger.info("Recall: " + metrics.recall());
		logger.info("F1 Score: " + metrics.fMeasure());

		// Stats by labels
		for (int i = 0; i < metrics.labels().length; i++) {
			logger.info(
					"Class " + metrics.labels()[i] + " precision: " + metrics.precision(metrics.labels()[i]) + "\n");
			logger.info("Class " + metrics.labels()[i] + " recall: " + metrics.recall(metrics.labels()[i]) + "\n");
			logger.info("Class " + metrics.labels()[i] + " F1 score: " + metrics.fMeasure(metrics.labels()[i]) + "\n");
		}

		// Weighted stats
		logger.info("Weighted precision: " + metrics.weightedPrecision() + "\n");
		logger.info("Weighted recall: " + metrics.weightedRecall() + "\n");
		logger.info("Weighted F1 score: " + metrics.weightedFMeasure() + "\n");
		logger.info("Weighted false positive rate: " + metrics.weightedFalsePositiveRate() + "\n");

		/* Write the Evaluation Metrics to a text file */
		tIF.append("Type: ").append(MlibCommon.MULTI_CLASSIFICATION).append("\n");
		tIF.append("Algorithm: ").append(params[2]).append("\n");
		tIF.append("Confusion matrix: \n").append(confusion.toString()).append("\n");
		tIF.append("Precision: " + MlibCommon.getFormatedDoubleValue(metrics.precision())).append("\n");
		tIF.append("Recall: ").append(MlibCommon.getFormatedDoubleValue(metrics.recall())).append("\n");
		tIF.append("F1 Score: ").append(MlibCommon.getFormatedDoubleValue(metrics.fMeasure())).append("\n");
		tIF.append("Num Of Labels: ").append(metrics.labels().length).append("\n");
		for (int i = 0; i < metrics.labels().length; i++) {
			tIF.append("Class ").append(metrics.labels()[i]).append(" precision: ")
					.append(MlibCommon.getFormatedDoubleValue(metrics.precision(metrics.labels()[i]))).append("\n");
			tIF.append("Class ").append(metrics.labels()[i]).append(" recall: ")
					.append(MlibCommon.getFormatedDoubleValue(metrics.recall(metrics.labels()[i]))).append("\n");
			tIF.append("Class ").append(metrics.labels()[i]).append(" F1 score: ")
					.append(MlibCommon.getFormatedDoubleValue(metrics.fMeasure(metrics.labels()[i]))).append("\n");
		}
		tIF.append("Weighted Precision: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedPrecision()))
				.append("\n");
		tIF.append("Weighted Recall: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedRecall()))
				.append("\n");
		tIF.append("Weighted F1 score: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedFMeasure()))
				.append("\n");
		tIF.append("Weighted False Positive Rate: ")
				.append(MlibCommon.getFormatedDoubleValue(metrics.weightedFalsePositiveRate())).append("\n");
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
			logger.error("MulticlassLogisticRegressionTesting: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}

}
