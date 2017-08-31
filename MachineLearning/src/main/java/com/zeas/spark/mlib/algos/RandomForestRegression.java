package com.zeas.spark.mlib.algos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;

import scala.Tuple2;

/**
 * This class is responsible to handle the Test and Train scenario for Random
 * Forest Regression Algorithm
 * 
 * @author Nisith.Nanda
 *
 */
public class RandomForestRegression extends AbstractMLAlgo implements Serializable {

	private static Logger logger = Logger.getLogger(RandomForestRegression.class);

	/*
	 * args[0] - train/test args[1] - schema args[2] -
	 * inputPath;outputPath;RandomForestClassification
	 * ;labelIndex;FeaturesIndex;numTrees
	 * ,maxDepth;MaxBins;featureSubsetStrategy;impurity;seed
	 */
	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {

		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		final Integer DEFAULT_NUMTREES = 3; // Use more in practice.
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		String impurity = "variance";
		final Integer DEFAULT_MAXDEPTH = 5;
		final Integer DEFAULT_MAXBINS = 32;
		int numTrees = DEFAULT_NUMTREES;
		int maxDepth = DEFAULT_MAXDEPTH;
		int maxBins = DEFAULT_MAXBINS;
		Integer seed = 12345;
		String hdfsPath = null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList = new ArrayList();
		if (args.length < 3) {
			handleException("Random Forest Regression: Insufficient number of arguments");
			return;
		}

		try {
			String[] params = args[2].split(";");
			// String [] params2 =args[3].split(";");
			hdfsPath = params[0];
			labelIndex = convertToInt(params[3]);
			String[] featureStrList = params[4].split(",");
			for (int i = 0; i < featureStrList.length; i++) {
				featureList.add(convertToInt(featureStrList[i]));
			}
			modelSaveLocation = params[1];
			if (params.length > 5) {
				numTrees = convertToInt(params[5]);
				maxDepth = convertToInt(params[6]);
				maxBins = convertToInt(params[7]);
				featureSubsetStrategy = params[8];
				impurity = params[9];
				seed = convertToInt(params[10]);

			}

		} catch (Exception e) {
			handleException("Random Forest Classification: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("RandomForestRegression Training");
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		trainingDataLabeled.cache();
		// train model
		final RandomForestModel model = RandomForest.trainRegressor(trainingDataLabeled, categoricalFeaturesInfo,
				numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

		model.save(sc.sc(), modelSaveLocation);
		/*
		 * JavaPairRDD<Double, String> predictionAndLabel =
		 * trainingDataLabeled.mapToPair(new PairFunction<LabeledPoint, Double,
		 * String>() {
		 * 
		 * public Tuple2<Double, String> call(LabeledPoint p) { return new
		 * Tuple2<Double, String>(model.predict(p.features()), "labels "
		 * +p.label()); } });
		 * 
		 * predictionAndLabel.saveAsTextFile(modelSaveLocation);
		 */

	}

	/*
	 * args[0] - test args[1] - schema args[2] -
	 * inputPath;outputPath;RandomForestClassification;savedmodellocation;
	 * labelIndex;FeaturesIndex
	 */
	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String args[]) {
		String hdfsPath = null;
		int labelIndex = 0;
		String outputPath = null;
		ArrayList<Integer> featureList = new ArrayList();
		String modelSavedLocation = null;

		String[] params = args[2].split(";");
		try {

			hdfsPath = params[0];
			outputPath = params[1];
			modelSavedLocation = params[3];
			labelIndex = convertToInt(params[4]);
			String[] featureStrList = params[5].split(",");
			for (int i = 0; i < featureStrList.length; i++) {
				featureList.add(convertToInt(featureStrList[i]));
			}
		} catch (Exception e) {
			handleException("Random forest Regression: Exception while processing arguments " + e.getMessage());

		}

		JavaSparkContext sc = getSparkContext("RandomForestRegression Testing");
		JavaRDD<String> testData = sc.textFile(hdfsPath);
		// testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));
		// getting the details of saved model
		final RandomForestModel savedModel = RandomForestModel.load(sc.sc(), modelSavedLocation);

		JavaRDD<String> results = testingDataLabeled.map(new Function<LabeledPoint, String>() {
			public String call(LabeledPoint point) {
				double prediction = savedModel.predict(point.features());
				return Double.toString(point.label()) + " , " + point.features().toString() + " , "
						+ Double.toString(prediction);
			}
		});
		// saving results to HDFS file
		results.saveAsTextFile(outputPath);
		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		// evaluation metrics

		StringBuilder tIF = new StringBuilder("");
		String sumModel = "";

		logger.info("Results stored in file : " + params[1]);
		/* Details of the Model */
		int nTrees = savedModel.numTrees();
		logger.info("Number of Decison Trees: " + nTrees);
		tIF.append("Number of Decison Trees: ").append(nTrees).append("\n");
		double treeWeights[] = savedModel.treeWeights();
		for (int i = 0; i < treeWeights.length; i++) {
			logger.info(
					"Weight assigned to tree " + (i + 1) + " is: " + MlibCommon.getFormatedDoubleValue(treeWeights[i]));
			tIF.append("Weight assigned to tree ").append(i + 1).append(" is: ")
					.append(MlibCommon.getFormatedDoubleValue(treeWeights[i])).append("\n");
		}
		// DecisionTreeModel nDTree[] = savedModel.trees();
		int nNodes = savedModel.totalNumNodes();
		logger.info("Number of Nodes: " + nNodes);
		tIF.append("Number of Nodes: ").append(nNodes).append("\n");
		// Summary of model in String
		sumModel = savedModel.toString();
		logger.info("Model Summary: " + sumModel);
		tIF.append("Model Summary: ").append(sumModel).append("\n");

		JavaRDD<Tuple2<Object, Object>> valueAndPred = testingDataLabeled
				.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						double prediction = savedModel.predict(point.features());
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				});
		tIF.append("Type: ").append(MlibCommon.REGRESSION).append("\n");
		tIF.append("Algorithm: ").append(params[2]).append("\n");
		RegressionMetrics metrics = new RegressionMetrics(valueAndPred.rdd());
		// Squared error
		logger.info("MSE: " + metrics.meanSquaredError());
		logger.info("RMSE: " + metrics.rootMeanSquaredError());
		// R-squared
		logger.info("R Squared: " + metrics.r2());

		// Mean absolute error
		logger.info("MAE: " + metrics.meanAbsoluteError());

		// Explained variance
		logger.info("Explained Variance: " + metrics.explainedVariance());

		/* Write the Evaluation Metrics to a txt file */
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
			logger.error("RandomForestRegression: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}

}
