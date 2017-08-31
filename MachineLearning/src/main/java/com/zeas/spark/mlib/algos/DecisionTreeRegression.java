package com.zeas.spark.mlib.algos;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
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
public class DecisionTreeRegression extends AbstractMLAlgo implements Serializable{

	/*
	 * args[0] - train/test
	 * args[1] - abcd
	 * args[2] - inputPath;outputPath;Decision_Tree_Regression;LabelIndex;FeaturesIndexes;maxDepth;maxBins;impurity
	 */
	private static Logger logger = Logger.getLogger(DecisionTreeRegression.class);
	
	/**
	 * This method is responsible to handle the train scenario
	 * 
	 * @param args
	 *            String array of all the arguments required
	 */
	public void train(String[] args) {
		
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		String impurity = "variance";
		final Integer DEFAULT_MAXDEPTH = 5;
		final Integer DEFAULT_MAXBINS = 32;
		int maxDepth = DEFAULT_MAXDEPTH;
		int maxBins = DEFAULT_MAXBINS;
		String hdfsPath=null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			handleException("DecisionTreeRegression: Insufficient number of arguments");
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
		}catch(Exception e){
			handleException("DecisionTreeRegression: Exception while processing arguments "+ e.getMessage());
			return;
		}
		
		//loading java spark context
		SparkConf sparkConf = new SparkConf().setAppName("DecisionTreeRegression");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		trainingDataLabeled.cache();

		final DecisionTreeModel model = DecisionTree.trainRegressor(trainingDataLabeled,
				  categoricalFeaturesInfo, impurity, maxDepth, maxBins);
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
		 * args[0] - test
		 * args[1] - abcd
		 * args[2] - inputPath;outputPath;DecisionTreeRegression;modelPath;LabelIndex;FeaturesIndexes
		 */
		JavaSparkContext sc = getSparkContext("DecisionTreeRegression Testing");
		SparkConf conf = new SparkConf().setAppName("DecisionTreeRegression Metrics");
		String modelPath=null;
		String resPath = null;
		String errorMessage = null;
		String ipPath=null;
		//String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<1){
			errorMessage = "DecisionTreeRegression: Insufficient number of arguments";
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
			errorMessage = "DecisionTreeRegression: Exception while processing arguments "+ e.getMessage();
			logger.error(errorMessage);
			return;
		}
		String evalPath = resPath + MlibCommon.FILE_EXTN;
		StringBuilder tIF = new StringBuilder("");
		String sumModel = null;
		final int label = labelIndex;		
		/*JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(spc, hdfsPath).toJavaRDD();
		// Split initial RDD into two... [60% training data, 40% testing data].
	    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
	    JavaRDD<LabeledPoint> training = splits[0].cache();
	    JavaRDD<LabeledPoint> test = splits[1];*/
		JavaRDD<String> testData = sc.textFile(ipPath);
		//testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));		
		testingDataLabeled.cache();

		final DecisionTreeModel savedModel = DecisionTreeModel.load(sc.sc(),modelPath);
		/*Details of the Model*/
		int nDepth = savedModel.depth();
		int nNodes = savedModel.numNodes();
	 	logger.info("Depth of tree: " + nDepth);		
	 	tIF.append("regressor\n").append( "Depth of tree: ").append(nDepth).append("\n");
	 	logger.info("Number of Nodes: " + nNodes);		
	 	tIF.append("Number of Nodes: ").append(nNodes).append("\n");
	 	//Summary of model in String
		sumModel = savedModel.toString();
	 	logger.info("Model Summary: " + sumModel);		
	 	tIF.append("Model Summary: ").append(sumModel).append("\n");
	 	
		JavaRDD<String> results = testingDataLabeled.map(
	   			 new Function<LabeledPoint, String>() {
				    	 public String call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				          return Double.toString(point.label()) + " , " + point.features().toString() +  " , "+  Double.toString(prediction) ;
				        }
				      }
				    );
		results.saveAsTextFile(resPath);
		logger.info("Results stored in file : "+ resPath);
		
		JavaRDD<Tuple2<Object,Object>> valueAndPred = testingDataLabeled.map(
				 new Function<LabeledPoint, Tuple2<Object,Object>>() {
				    	 public Tuple2<Object,Object> call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				         return new Tuple2<Object,Object>(prediction,point.label());
				        }
				      }
				    );
		
		RegressionMetrics metrics = new RegressionMetrics(valueAndPred.rdd());
		  // Squared error
		   logger.info("MSE: "+metrics.meanSquaredError()+"\n");
		   logger.info("RMSE: "+metrics.rootMeanSquaredError()+"\n");
		    // R-squared
		   logger.info("R Squared: "+metrics.r2()+"\n");

		    // Mean absolute error
		   logger.info("MAE: "+ metrics.meanAbsoluteError()+"\n");

		    // Explained variance
		   logger.info("Explained Variance: "+ metrics.explainedVariance() +"\n");
		    
		    /*Write the Evaluation Metrics to a txt file*/
			    tIF.append( "MSE: ").append( MlibCommon.getFormatedDoubleValue(metrics.meanSquaredError())).append("\n");
			    tIF.append( "RMSE: ").append(MlibCommon.getFormatedDoubleValue(metrics.rootMeanSquaredError())).append("\n");
			    tIF.append( "R Squared: ").append(MlibCommon.getFormatedDoubleValue(metrics.r2())).append("\n");
			    tIF.append( "MAE: ").append(MlibCommon.getFormatedDoubleValue(metrics.meanAbsoluteError())).append("\n");
			    tIF.append(  "Explained Variance: ").append(MlibCommon.getFormatedDoubleValue(metrics.explainedVariance())).append("\n");
			    logger.info("Evaluation Metrics stored in "+evalPath);
			    Path newFilePath=new Path(evalPath);
		    FileSystem hdfs;
			try {
				hdfs = FileSystem.get(new Configuration());
			    hdfs.createNewFile(newFilePath);
			    FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
			    fsOutStream.write(tIF.toString().getBytes());
			    fsOutStream.close();
			} catch (Exception e) {
				logger.error("DecisionTreeRegression: Failure in writing to file");
				e.printStackTrace();
			}
			sc.close();
	}

}
