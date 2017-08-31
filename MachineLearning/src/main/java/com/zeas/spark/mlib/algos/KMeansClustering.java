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
import org.apache.spark.mllib.clustering.KMeans;
//import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetVectorsData;

/**
 * This class is responsible to handle KMeans Clustering Algorithms Train and
 * Test scenario
 * 
 * @author Nisith.Nanda
 *
 */
public class KMeansClustering extends AbstractMLAlgo implements Serializable {
	/*
	 * args[0] - train/test args[1] - schema args[2] -
	 * inputpath;outputPath;Kmeans_Clustering;FeaturesIndexes;numClasses;
	 * numIterations;
	 *
	 */
	private static Logger logger = Logger.getLogger(KMeansClustering.class);

	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {

		final int DEFAULT_NUM_CLASSES = 5;
		final int DEFAULT_NUM_ITERATIONS = 100;
		String hdfsPath = null;
		String modelSaveLocation = null;
		ArrayList<Integer> featureList = new ArrayList();
		int numClasses = DEFAULT_NUM_CLASSES;
		int numIterations = DEFAULT_NUM_ITERATIONS;
		if (args.length < 3) {
			handleException("Kmeans Regression: Insufficient number of arguments");
			return;
		}
		try {
			String[] params = args[2].split(";");
			// String [] params2 =args[3].split(";");
			hdfsPath = params[0];
			String[] featureStrList = params[3].split(",");
			for (int i = 0; i < featureStrList.length; i++) {
				featureList.add(convertToInt(featureStrList[i]));
			}
			modelSaveLocation = params[1];
			if (params.length >= 5) {
				numClasses = convertToInt(params[4]);
				numIterations = convertToInt(params[5]);

			}

		} catch (Exception e) {
			handleException("Linear Regression: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("Kmeans Clustering");
		// GetLabeledData gl =new GetLabeledData();

		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<Vector> trainingDataLabeled = trainingData.map(new GetVectorsData(featureList));
		// JavaRDD<LabeledPoint> trainingDataLabeled =
		// getLabeledData(trainingData, labelIndex, featureList, delimiter);
		trainingDataLabeled.cache();

		// Building the model
		final KMeansModel model = KMeans.train(trainingDataLabeled.rdd(), numClasses, numIterations);

		saveModel(model, modelSaveLocation, sc);

	}

	public String saveModel(KMeansModel model, String modelSaveLocation, JavaSparkContext sc) {
		model.save(sc.sc(), modelSaveLocation);
		printMessage("Model saved at " + modelSaveLocation);
		return modelSaveLocation;

	}

	/*
	 * args[0] - LinearRegression args[1] - train/test args[2] -
	 * inputpath;outputPath;Kmeans_Clustering;savedModelLocation;
	 * FeaturesIndexess;
	 *
	 */
	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String[] args) {

		String hdfsPath = null;
		String modelSaveLocation = null;
		String outputPath = null;
		ArrayList<Integer> featureList = new ArrayList();
		if (args.length < 3) {
			handleException("Kmeans Regression: Insufficient number of arguments");
			return;
		}
		String[] params = args[2].split(";");
		try {

			// String [] params2 =args[3].split(";");
			hdfsPath = params[0];
			String[] featureStrList = params[4].split(",");
			for (int i = 0; i < featureStrList.length; i++) {
				featureList.add(convertToInt(featureStrList[i]));
			}
			modelSaveLocation = params[3];
			outputPath = params[1];

		} catch (Exception e) {
			handleException("Linear Regression: Exception while processing arguments " + e.getMessage());
		}

		JavaSparkContext sc = getSparkContext("Kmeans Clustering");
		JavaRDD<String> testingData = sc.textFile(hdfsPath);
		JavaRDD<Vector> testingDataLabeled = testingData.map(new GetVectorsData(featureList));
		testingDataLabeled.cache();
		final KMeansModel model = KMeansModel.load(sc.sc(), modelSaveLocation);

		JavaRDD<String> results = testingDataLabeled.map(new Function<Vector, String>() {
			public String call(Vector vector) throws Exception {
				double prediction = model.predict(vector.toDense());
				return vector.toDense().toString() + " , " + Double.toString(prediction);
			}

		});

		results.saveAsTextFile(outputPath);

		// evaluation metrics...
		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");

		logger.info("Results stored in file : " + outputPath);

		Vector[] cCenter = model.clusterCenters();
		logger.info("Cluster Centers:");
		for (int j = 0; j < cCenter.length; j++) {
			logger.info("Cluster " + (j + 1) + " :" + cCenter[j].toString());
		}
		int cK = model.k();
		logger.info("Number of Clusters: " + cK);
		// double WSSSE = savedModel.computeCost(parsedData.rdd());
		// logger.info("Within Set Sum of Squared Errors = " + WSSSE);
		// savedModel.predict(parsedData.rdd());

		tIF.append("Type: ").append(MlibCommon.CLUSTERING).append("\n");
		tIF.append("Algorithm: ").append(params[2]).append("\n");
		tIF.append("Cluster Centers: ");
		for (int j = 0; j < cCenter.length; j++) {
			tIF.append("Cluster ").append(j + 1).append(" :");
			for (int i = 0; i < cCenter[j].toArray().length; i++) {
				if (i + 1 == cCenter[j].toArray().length) {
					tIF.append(MlibCommon.getFormatedDoubleValue(cCenter[j].toArray()[i])).append("\n");
				} else {
					tIF.append(MlibCommon.getFormatedDoubleValue(cCenter[j].toArray()[i])).append(",");
				}
			}
		}
		tIF.append("Number of Clusters: ").append(cK).append("\n");

		// tIF = tIF + "Within Set Sum of Squared Errors = " + WSSSE);
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
			logger.error("KMeansClustering: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}

}
