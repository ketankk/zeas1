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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;


import scala.Tuple2;

import com.zeas.spark.mlib.common.MlibCommon;
import com.zeas.spark.mlib.functions.GetLabeledData;


/**
 * This class is responsible to handle the Test and Train scenario for Decision Tree
 *  CLassification Algorithm
 * 
 * @author Nisith.Nanda
 *
 */
public class DecisionTreeClassification extends AbstractMLAlgo implements Serializable{

	/*
	 * args[0] - train/test
	 * args[1] - abcd
	 * args[2] - inputPath;outputPath;Decision_Tree_Classification;LabelIndex;FeaturesIndexes;maxDepth;maxBins;impurity
	 *
	 */
	private static Logger logger = Logger.getLogger(DecisionTreeClassification.class);
	
	
	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {

		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		String impurity = "gini";
		final Integer DEFAULT_MAXDEPTH = 5;
		final Integer DEFAULT_CLASSES = 2;
		final Integer DEFAULT_MAXBINS = 32;
		int maxDepth = DEFAULT_MAXDEPTH;
		int numClasses= DEFAULT_CLASSES;
		int maxBins = DEFAULT_MAXBINS;
		String hdfsPath=null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			handleException("DecisionTreeClassification: Insufficient number of arguments");
			return;
		}
		
		try{
			String [] params =args[2].split(";");
			hdfsPath=params[0];
			labelIndex=convertToInt(params[3]);
			String[] featureStrList = params[4].split(",");
			for(int i=0;i<featureStrList.length;i++){
				featureList.add(convertToInt(featureStrList[i]));
			}
			modelSaveLocation=params[1];
			if(params.length>5){
				impurity = params[7];
			    maxDepth=convertToInt(params[5]);
			    maxBins=convertToInt(params[6]);
			}
/*			if(params.length>8){
			   numClasses=convertToInt(params[8]);
			}*/
		}catch(Exception e){
			handleException("DecisionTreeClassification: Exception while processing arguments "+ e.getMessage());
			return;
		}
		
		//loading java spark context
		JavaSparkContext sc = getSparkContext("DecisionTreeClassification");
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		//calculating number of class in label
		final int label = labelIndex;
		logger.info("label is >>>>>>>>>>>>>>>>>"+label);
		JavaRDD<Integer> labelData = trainingData.map(new Function<String,Integer>(){
			public Integer call(String line)  {
				logger.info("line is >>>>>>>>>>>>>>>"+line);
				String[] parts = line.split(",");
				logger.info(parts.length);				
					return Integer.parseInt(parts[label]);							
			}
			  });
		
		 JavaRDD<Integer> distinctValues = labelData.distinct();
		List<Integer> list = distinctValues.collect();
		int maxValue =0;
		Iterator<Integer> iter = list.iterator();
		int currentVal;
		while (iter.hasNext()) {
             currentVal =iter.next();
			if (currentVal >= maxValue) {
				maxValue = currentVal;
			}

		}
		 numClasses =maxValue+1;
		 logger.info("num of classes are >>>>>>>>>>>>>>"+numClasses);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		trainingDataLabeled.cache();

		// Building the model
		final DecisionTreeModel model = DecisionTree.trainClassifier(trainingDataLabeled, numClasses,categoricalFeaturesInfo, impurity, maxDepth, maxBins);		
		model.save(sc.sc(), modelSaveLocation);
		logger.info("Model saved at : "+ modelSaveLocation);
		sc.close();
	}
	
	/**
	 * This method is responsible to handle the test scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */

	public void test(String[] args) {
		/*
		 * args[0] - train/test
		 * args[1] - abcd
		 * args[2] - inputPath;outputPath;DecisionTreeClassification;modelPath;LabelIndex;FeaturesIndexes;
		 *
		 */
		JavaSparkContext sc = getSparkContext("DecisionTreeClassification Testing");
		SparkConf conf = new SparkConf().setAppName("DecisionTreeClassification Metrics");
		String modelPath=null;
		String errorMessage = null;
		String ipPath=null;
		String resPath = null;
		int nClass = 0;
		//String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			errorMessage = "DecisionTreeClassification: Insufficient number of arguments";
			logger.error(errorMessage);
			return;
		}
		
		try{
			String [] params =args[2].split(";");
			ipPath=params[0];
			modelPath=params[3];
			labelIndex=Integer.parseInt(params[4]);
			String[] featureStrList = params[5].split(",");
			for(int i=0;i<featureStrList.length;i++){
				featureList.add(Integer.parseInt(featureStrList[i]));
			}
			resPath = params[1];
		}catch(Exception e){
			errorMessage = "DecisionTreeClassification: Exception while processing arguments "+ e.getMessage();
			logger.error(errorMessage);
			return;
		}
		StringBuilder tIF = new StringBuilder("");
		String evalPath = resPath + MlibCommon.FILE_EXTN;
		String sumModel = null;
		final int label = labelIndex;		/*JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(spc, hdfsPath).toJavaRDD();
		// Split initial RDD into two... [60% training data, 40% testing data].
	    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
	    JavaRDD<LabeledPoint> training = splits[0].cache();
	    JavaRDD<LabeledPoint> test = splits[1];*/
		JavaRDD<String> testData = sc.textFile(ipPath);
		//testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));
		JavaRDD<Integer> labelData = testData.map(new Function<String,Integer>(){
			public Integer call(String line)  {
				String[] parts = line.split(",");
					return Integer.parseInt(parts[label]);							
			}
			  });
		final DecisionTreeModel savedModel = DecisionTreeModel.load(sc.sc(),modelPath);
		/*Details of the Model*/
		int nDepth = savedModel.depth();
		int nNodes = savedModel.numNodes();
	 	logger.info("Depth of tree: " + nDepth);		
	 	tIF.append("classifier\n").append("Depth of tree: ").append( nDepth).append("\n");
	 	logger.info("Number of Nodes: " + nNodes);		
	 	tIF.append("Number of Nodes: ").append(nNodes).append("\n");
	 	//Summary of model in String
		sumModel = savedModel.toString();
	 	logger.info("Model Summary: " + sumModel);		
	 	tIF = tIF.append("Model Summary: ").append(sumModel).append( "\n");
	 	
		JavaRDD<String> results = testingDataLabeled.map(
	   			 new Function<LabeledPoint, String>() {
				    	 public String call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				          return point.features().toString() +  " , "+ Double.toString(point.label()) + " , "+ Double.toString(prediction) ;
				        }
				      }
				    );
		results.saveAsTextFile(resPath);
		logger.info("Results stored in file: "+ resPath);
		
		JavaRDD<Tuple2<Object,Object>> valueAndPred = testingDataLabeled.map(
				 new Function<LabeledPoint, Tuple2<Object,Object>>() {
				    	 public Tuple2<Object,Object> call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				         return new Tuple2<Object,Object>(prediction,point.label());
				        }
				      }
				    );
		
		JavaRDD<Integer> distinctValues = labelData.distinct();
		List<Integer> list = distinctValues.collect();
		int maxValue =0;
		Iterator<Integer> iter = list.iterator();
		int currentVal;
		while (iter.hasNext()) {
             currentVal =iter.next();
			if (currentVal >= maxValue) {
				maxValue = currentVal;
			}

		}
		 nClass =maxValue+1;
		logger.info("No. of Classes are: "+ nClass);
	 	tIF = tIF.append( "No. of Classes are: ").append( nClass).append("\n");
	 	if(nClass==2)//binary classification 
		{
				//Evaluation Metrics (Binary Classification)
			 	BinaryClassificationMetrics bcmetrics = new BinaryClassificationMetrics(valueAndPred.rdd());		
			 	logger.info("Area under ROC curve: " + bcmetrics.areaUnderROC());
			 	logger.info("Number of Bins: " + bcmetrics.numBins());
			 
			 	//Experiment with the next line
//			 	JavaRDD<Tuple2<Object,Object>>scoreAndLabels = bcmetrics.scoreAndLabels().toJavaRDD();
			 
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

			    /*Write the Evaluation Metrics to a txt file*/
					 tIF.append("Area under ROC curve: ").append(MlibCommon.getFormatedDoubleValue(bcmetrics.areaUnderROC())).append( "\n");
				 	 tIF.append("Number of Bins: ").append(MlibCommon.getFormatedDoubleValue(bcmetrics.numBins())).append("\n");
				     //tIF.append("Precision by threshold: ").append(precision.collect()).append("\n");
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
				    // tIF.append("Recall by threshold: ").append(recall.collect()).append("\n");
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
				    // tIF.append("F1 Score by threshold: " ).append(f1Score.collect()).append("\n");
				     tIF.append("F1 Score by threshold: " );
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
				    // tIF.append("F2 Score by threshold: ").append(f2Score.collect()).append("\n");
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
				    // tIF.append("Precision-recall curve: ").append(prc.collect()).append("\n");
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
				    // tIF.append("ROC curve: " + roc.collect()).append("\n");
				     tIF.append("ROC curve: " );
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
				     tIF.append("Area under precision-recall curve: ").append(MlibCommon.getFormatedDoubleValue(bcmetrics.areaUnderPR())).append("\n");
				    logger.info("Evaluation Metrics stored in "+ evalPath);
		}
		else if(nClass>2){ //more than two classes
			//Evaluation Metrics (Multiple Classes Classification)
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
		    	logger.info("Class "+ metrics.labels()[i]+" precision: "+metrics.precision(metrics.labels()[i])+"\n");
		    	logger.info("Class "+ metrics.labels()[i]+" recall: "+metrics.recall(metrics.labels()[i])+"\n");
		    	logger.info("Class "+ metrics.labels()[i]+" F1 score: "+metrics.fMeasure(metrics.labels()[i])+"\n");
		    }

		    //Weighted stats
		   logger.info("Weighted precision: "+metrics.weightedPrecision()+"\n");
		   logger.info("Weighted recall: "+ metrics.weightedRecall()+"\n");
		   logger.info("Weighted F1 score: "+metrics.weightedFMeasure()+"\n" );
		   logger.info("Weighted false positive rate: "+metrics.weightedFalsePositiveRate()+"\n");
		    
		    /*Write the Evaluation Metrics to a txt file*/
				tIF.append("Confusion matrix: \n").append(confusion.toString()).append( "\n");
			    tIF.append("Precision: ").append(MlibCommon.getFormatedDoubleValue(metrics.precision())).append("\n");
			    tIF.append("Recall: ").append(MlibCommon.getFormatedDoubleValue(metrics.recall())).append("\n");
			    tIF.append("F1 Score: ").append(MlibCommon.getFormatedDoubleValue(metrics.fMeasure())).append("\n");
			    for (int i = 0; i < metrics.labels().length; i++) {
			        tIF.append("Class ").append(metrics.labels()[i]).append(" precision: ").append(MlibCommon.getFormatedDoubleValue(metrics.precision(metrics.labels()[i]))).append("\n");
			        tIF.append("Class ").append(metrics.labels()[i]).append(" recall: ").append(MlibCommon.getFormatedDoubleValue(metrics.recall(metrics.labels()[i]))).append("\n");
			        tIF.append("Class ").append(metrics.labels()[i]).append(" F1 score: ").append(MlibCommon.getFormatedDoubleValue(metrics.fMeasure(metrics.labels()[i]))).append("\n");
			    }
			    tIF.append("Weighted precision: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedPrecision())).append("\n");
			    tIF.append("Weighted recall: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedRecall())).append("\n");
			    tIF.append("Weighted F1 score: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedFMeasure())).append("\n") ;
			    tIF.append("Weighted false positive rate: ").append(MlibCommon.getFormatedDoubleValue(metrics.weightedFalsePositiveRate())).append("\n");
			    logger.info("Evaluation Metrics stored in "+ evalPath);
		}
	    Path newFilePath=new Path(evalPath);
	    FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
		    hdfs.createNewFile(newFilePath);
		    FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		    fsOutStream.write(tIF.toString().getBytes());
		    fsOutStream.close();
		} catch (Exception e) {
			logger.error("DecisionTreeClassification: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();
	}

}
