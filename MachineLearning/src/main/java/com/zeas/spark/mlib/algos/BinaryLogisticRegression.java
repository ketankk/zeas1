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
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;

import scala.Tuple2;

/**
 * This class is responsible to handle Binary Logistic Regression scenario
 * 
 * @author Nisith.Nanda
 *
 */
public class BinaryLogisticRegression extends AbstractMLAlgo implements Serializable {

	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(BinaryLogisticRegression.class);

	/*
	 * ================================ OLD ===============================
	 * Arguments to the function should be as follow Argument 1: name of the
	 * Algorithm e,g, "logisticregression" Argument 2: HDFS Path to read from
	 * such as hdfs:/user/19491/iotdata/ Argument 3: label Index such as 10
	 * Argument 4: feature list comma separated e.g. 0,1,4,5,6,7,8,9 (start with
	 * 0) Argument 5: Location to save Model Argument 6 (Optional): Step size
	 * for logistic regression e.g. stepsize=0.02 (default is 1 if not provided)
	 * Argument 7 (Optional): Number of iteration for logistic regression e.g
	 * numiterations=1000 (Default is 100 if not provided) Argument 8
	 * (Optional): Delimiter (Default is ,). e.g. delimiter=, Argument 9
	 * (optional): Header (Default is false) e.g. header=true
	 */

	/*
	 * args[0] - LinearRegression args[1] - train/test args[2] -
	 * inputpath;outputPath;binary_logistic_regression;labelIndex;FeaturesIndex;
	 * stepsize; numIterations;minBatchFraction;regParam;intercept;
	 * 
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
		final String DEFAULT_DELIMITER = ",";
		String hdfsPath = null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		double stepSize = DEFAULT_STEP_SIZE;
		int numIterations = DEFAULT_NUM_ITERATION;
		Double minBatchFraction = null;
		Double regParam = null;
		boolean intercept = true;
		// String delimiter = DEFAULT_DELIMITER;
		// boolean hasHeader=true;
		ArrayList<Integer> featureList = new ArrayList<Integer>();

		if (args.length < 3) {
			handleException(
					"Binary Logistic Regression: Insufficient number of arguments. Expected 3 got " + args.length);
			return;
		}
		try {
			String[] params = args[2].split(";");
			if (params.length < 10) {
				handleException(
						"Binary Logistic Regression: Insufsficient number of arguments for the third argument. \n arg = "
								+ params);
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
			regParam = new Double(params[8]);
			intercept = new Boolean(params[9]);
		} catch (Exception e) {
			handleException("Binary logistic Regression: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("BinaryLogisticRegressionTraining");

		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		// JavaRDD<LabeledPoint> trainingDataLabeled =
		// getLabeledData(trainingData, labelIndex, featureList, delimiter);

		trainingDataLabeled.cache();

		// Building the model
		// final LogisticRegressionModel model =
		// LogisticRegressionWithSGD.train(trainingDataLabeled.rdd(),
		// numIterations, stepSize);
		LogisticRegressionWithSGD alg = new LogisticRegressionWithSGD();
		alg.optimizer().setNumIterations(numIterations);
		alg.optimizer().setStepSize(stepSize);
		alg.optimizer().setMiniBatchFraction(minBatchFraction);
		alg.optimizer().setRegParam(regParam);
		alg.setIntercept(intercept);
		LogisticRegressionModel model = alg.run(trainingDataLabeled.rdd());
		saveModel(model, modelSaveLocation, sc);

		logger.info("Model Weights : " + model.weights());
		logger.info("Model Intercept : " + model.intercept());
	}

	// saving the model...
	private void saveModel(LogisticRegressionModel model, String modelSaveLocation, JavaSparkContext sc) {
		model.save(sc.sc(), modelSaveLocation);
		printMessage("Model saved at " + modelSaveLocation);
	}

	/*
	 * args[0] - test args[1] - schema args[2] -
	 * inputpath;output;binary_logistic_regression;savedModellocation;labelIndex
	 * ;FeaturesIndex;
	 */

	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String[] args) {
		JavaSparkContext sc = getSparkContext("BinaryLogisticRegressionTesting");
		String[] params = args[2].split(";");
		String hdfsPath = params[0];
		String outputPath = params[1];
		String modelSavedLocation = params[3];
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
		final LogisticRegressionModel savedModel = LogisticRegressionModel.load(sc.sc(), modelSavedLocation);
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

		/*
		 * logger.info("Saved Model Weights : "+savedModel.weights());
		 * logger.info("Saved Model Intercept : "+savedModel.intercept());
		 */

		// evaluation metrics

		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");
		String sumModel = "";

		logger.info("Results stored in file : " + params[1]);

		JavaRDD<Tuple2<Object, Object>> valueAndPred = testingDataLabeled
				.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						double prediction = savedModel.predict(point.features());
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				});
		// Evaluation Metrics (Binary Classification)
		BinaryClassificationMetrics bcmetrics = new BinaryClassificationMetrics(valueAndPred.rdd());
		logger.info("Area under ROC curve: " + bcmetrics.areaUnderROC());
		logger.info("Number of Bins: " + bcmetrics.numBins());

		/*
		 * //Experiment with the next line
		 * JavaRDD<Tuple2<Object,Object>>scoreAndLabels =
		 * bcmetrics.scoreAndLabels().toJavaRDD();
		 */
		// Precision by threshold
		JavaRDD<Tuple2<Object, Object>> precision = bcmetrics.precisionByThreshold().toJavaRDD();
		logger.info("Precision by threshold: " + precision.collect());

		// Recall by threshold
		JavaRDD<Tuple2<Object, Object>> recall = bcmetrics.recallByThreshold().toJavaRDD();
		logger.info("Recall by threshold: " + recall.collect());

		// F Score by threshold
		JavaRDD<Tuple2<Object, Object>> f1Score = bcmetrics.fMeasureByThreshold().toJavaRDD();
		logger.info("F1 Score by threshold: " + f1Score.collect());
		// F Score by threshold with Beta = 2.0
		JavaRDD<Tuple2<Object, Object>> f2Score = bcmetrics.fMeasureByThreshold(2.0).toJavaRDD();
		logger.info("F2 Score by threshold: " + f2Score.collect());

		// Precision-recall curve
		JavaRDD<Tuple2<Object, Object>> prc = bcmetrics.pr().toJavaRDD();
		logger.info("Precision-recall curve: " + prc.collect());

		// ROC Curve
		JavaRDD<Tuple2<Object, Object>> roc = bcmetrics.roc().toJavaRDD();
		logger.info("ROC curve: " + roc.collect());

		// AUPRC
		logger.info("Area under precision-recall curve: " + bcmetrics.areaUnderPR());

		/* Write the Evaluation Metrics to a txt file */
		tIF.append("Type: ").append(MlibCommon.BINARY_CLASSIFICATION).append("\n");
		tIF.append("Algorithm: ").append(params[2]).append("\n");
		tIF.append("Area under ROC curve: ").append(MlibCommon.getFormatedDoubleValue(bcmetrics.areaUnderROC()))
				.append("\n");
		tIF.append("Number of Bins: ").append(bcmetrics.numBins()).append("\n");
		tIF.append("Precision by threshold: ");
		for (int i = 0; i < precision.collect().size(); i++) {
			Tuple2 data = precision.collect().get(i);

			if ((i + 1) == precision.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("Recall by threshold: ");
		for (int i = 0; i < recall.collect().size(); i++) {
			Tuple2 data = recall.collect().get(i);
			if ((i + 1) == recall.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("F1 Score by threshold: ");
		for (int i = 0; i < f1Score.collect().size(); i++) {
			Tuple2 data = f1Score.collect().get(i);
			if ((i + 1) == f1Score.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("F2 Score by threshold: ");
		for (int i = 0; i < f2Score.collect().size(); i++) {
			Tuple2 data = f2Score.collect().get(i);
			if ((i + 1) == f2Score.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("Precision-recall curve: ");
		for (int i = 0; i < prc.collect().size(); i++) {
			Tuple2 data = prc.collect().get(i);
			if ((i + 1) == prc.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("ROC curve: ");
		for (int i = 0; i < roc.collect().size(); i++) {
			Tuple2<Object, Object> data = roc.collect().get(i);
			if ((i + 1) == roc.collect().size()) {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue())).append("\n");
			} else {
				tIF.append(MlibCommon.getFormatedDoubleValue(((Double) data._1()).doubleValue())).append(",")
						.append(MlibCommon.getFormatedDoubleValue(((Double) data._2()).doubleValue()))
						.append(MlibCommon.DATA_DELIM);
			}
		}
		tIF.append("Area under precision-recall curve: ")
				.append(MlibCommon.getFormatedDoubleValue(bcmetrics.areaUnderPR())).append("\n");
		logger.info("Evaluation Metrics stored in " + evalPath);
		// tIF = tIF + "ROC curve array: " + roc.toArray() + "\n";
		Path newFilePath = new Path(evalPath);
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
			hdfs.createNewFile(newFilePath);
			FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
			fsOutStream.write(tIF.toString().getBytes());
			fsOutStream.close();
		} catch (Exception e) {
			logger.error("BinaryLogisticRegression: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}

}
