package com.zeas.spark.mlib.algos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;
import com.zeas.spark.mlib.functions.GetNumClasses;

import scala.Tuple2;

/**
 * This class is responsible to handle the Test and Train scenario for Random
 * Forest CLassification Algorithm
 * 
 * @author Nisith.Nanda
 *
 */
public class RandomForestClassification extends AbstractMLAlgo implements Serializable {

	/*
	 * args[0] - train/test args[1] - schema args[2] -
	 * inputPath;outputPath;RandomForestClassification;labelIndex;FeaturesIndex;
	 * numOfTrees;maxDepth;MaxBins;featureSubsetStrategy;impurity;seed
	 */
	private static Logger logger = Logger.getLogger(RandomForestClassification.class);

	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {

		// Integer numClasses = 4;
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		final Integer DEFAULT_NUMTREES = 3; // Use more in practice.
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		String impurity = "gini";
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
			handleException("Random Forest Classification: Insufficient number of arguments");
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

		// loading java spark context
		JavaSparkContext sc = getSparkContext("RandomForestClassification Training");
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		// calculating number of classes in label
		/*
		 * final int label = labelIndex; logger.info(
		 * "label is >>>>>>>>>>>>>>>>>>>>"+label); JavaRDD<Integer> labelData =
		 * trainingData.map(new Function<String,Integer>(){ public Integer
		 * call(String line) { logger.info("line is >>>>>>>>>>>>>>>"+line);
		 * String[] parts = line.split(","); logger.info(parts.length);
		 * 
		 * return Integer.parseInt(parts[label]);
		 * 
		 * 
		 * } });
		 * 
		 * JavaRDD<Integer> distinctValues = labelData.distinct(); List<Integer>
		 * list = distinctValues.collect(); int maxValue =0; Iterator<Integer>
		 * iter = list.iterator(); while ( iter.hasNext() ) {
		 * 
		 * if(iter.next()>= maxValue){ maxValue =iter.next(); }
		 * 
		 * } int numClasses =maxValue+1; logger.info(
		 * "num of classes are >>>>>>>>>>>>>>"+numClasses);
		 */
		GetNumClasses getNumClasses = new GetNumClasses(trainingData, labelIndex);
		int numClasses = getNumClasses.numClasses();

		// create java Rdd of LabeledPoint
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		trainingDataLabeled.cache();
		// training the model
		final RandomForestModel model = RandomForest.trainClassifier(trainingDataLabeled, numClasses,
				categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

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
	 * args[0] - train/test args[1] - schema args[2] -
	 * inputPath;outputPath;RandomForestClassification;labelIndex;FeaturesIndex;
	 */
	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void test(String[] args) {

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
			handleException("Random forest classification: Exception while processing arguments " + e.getMessage());

		}

		JavaSparkContext sc = getSparkContext("RandomForestClassificationTesting");
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

		// evaluation metrics....

		String evalPath = outputPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");
		String sumModel = "";
		final int label = labelIndex;
		JavaRDD<Integer> labelData = testData.map(new Function<String, Integer>() {
			public Integer call(String line) {
				String[] parts = line.split(",");
				return Integer.parseInt(parts[label]);
			}
		});
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

		JavaRDD<Integer> distinctValues = labelData.distinct();
		List<Integer> list = distinctValues.collect();
		int maxValue = 0;
		Iterator<Integer> iter = list.iterator();
		int currentVal;
		while (iter.hasNext()) {
			currentVal = iter.next();
			if (currentVal >= maxValue) {
				maxValue = currentVal;
			}

		}
		int nClass = maxValue + 1;
		logger.info("No. of Classes are: " + nClass);
		if (nClass == 2) // binary classification
		{
			// Evaluation Metrics (Binary Classification)
			BinaryClassificationMetrics bcmetrics = new BinaryClassificationMetrics(valueAndPred.rdd());
			logger.info("Area under ROC curve: " + bcmetrics.areaUnderROC());
			logger.info("Number of Bins: " + bcmetrics.numBins());

			// Experiment with the next line
			// JavaRDD<Tuple2<Object,Object>>scoreAndLabels =
			// bcmetrics.scoreAndLabels().toJavaRDD();

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
			/*
			 * tIF.append("Precision by threshold: " + precision.collect() +
			 * "\n"; tIF.append("Recall by threshold: " + recall.collect() +
			 * "\n"; tIF.append("F1 Score by threshold: " + f1Score.collect() +
			 * "\n"; tIF.append("F2 Score by threshold: " + f2Score.collect() +
			 * "\n"; tIF.append("Precision-recall curve: " + prc.collect() +
			 * "\n"; tIF.append("ROC curve: " + roc.collect() + "\n";
			 */
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
				Tuple2 data = roc.collect().get(i);
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
		} else if (nClass > 2) { // more than two classes
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
				logger.info("Class " + metrics.labels()[i] + " precision: " + metrics.precision(metrics.labels()[i])
						+ "\n");
				logger.info("Class " + metrics.labels()[i] + " recall: " + metrics.recall(metrics.labels()[i]) + "\n");
				logger.info(
						"Class " + metrics.labels()[i] + " F1 score: " + metrics.fMeasure(metrics.labels()[i]) + "\n");
			}

			// Weighted stats
			logger.info("Weighted precision: " + metrics.weightedPrecision() + "\n");
			logger.info("Weighted recall: " + metrics.weightedRecall() + "\n");
			logger.info("Weighted F1 score: " + metrics.weightedFMeasure() + "\n");
			logger.info("Weighted false positive rate: " + metrics.weightedFalsePositiveRate() + "\n");

			/* Write the Evaluation Metrics to a txt file */
			tIF.append("Type: ").append(MlibCommon.MULTI_CLASSIFICATION).append("\n");
			tIF.append("Algorithm: ").append(params[2]).append("\n");
			tIF.append("Confusion matrix: \n").append(confusion.toString()).append("\n");
			tIF.append("Precision: ").append(MlibCommon.getFormatedDoubleValue(metrics.precision())).append("\n");
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
		}
		Path newFilePath = new Path(evalPath);
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
			hdfs.createNewFile(newFilePath);
			FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
			fsOutStream.write(tIF.toString().getBytes());
			fsOutStream.close();
		} catch (Exception e) {
			logger.error("RandomForestClassification: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();

	}
}
