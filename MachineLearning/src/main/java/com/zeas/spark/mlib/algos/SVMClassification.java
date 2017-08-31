package com.zeas.spark.mlib.algos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import scala.Tuple2;

import com.zeas.spark.mlib.functions.GetLabeledData;

public class SVMClassification extends AbstractMLAlgo implements Serializable{

	/*
	 * args[0] - train/test
	 * args[1] - abcd
	 * args[2] - inputPath;outputPath;SVM_Classification;LabelIndex;FeaturesIndexes;numIterations;stepSize;regParam;miniBatchFraction;
	 */

	public void train(String[] args) {
		final Integer DEFAULT_ITERATIONS = 5;
		final Double DEFAULT_STEPSIZE = 1.0;
		final Double DEFAULT_REGP = 0.01;
		final Double DEFAULT_MINIBFRAC = 1.0;
		int numIterations = DEFAULT_ITERATIONS;
		double stepSize = DEFAULT_STEPSIZE;
		double regParam = DEFAULT_REGP;
		double miniBatchFrac = DEFAULT_MINIBFRAC;
		String hdfsPath=null;
		String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			handleException("SVMClassification: Insufficient number of arguments");
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
				regParam = convertToDouble(params[7]);
				numIterations=convertToInt(params[5]);
				stepSize=convertToDouble(params[6]);
				miniBatchFrac = convertToDouble(params[8]);
			}
		}catch(Exception e){
			handleException("SVMClassification: Exception while processing arguments "+ e.getMessage());
			return;
		}
		
		//loading java spark context
		SparkConf sparkConf = new SparkConf().setAppName("SVMClassification");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> trainingData = sc.textFile(hdfsPath);
		JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
		trainingDataLabeled.cache();	
		
	    final SVMModel model = SVMWithSGD.train(trainingDataLabeled.rdd(), numIterations, stepSize, regParam, miniBatchFrac);

		model.save(sc.sc(), modelSaveLocation);
		logger.info("Model saved at : "+ modelSaveLocation);
		sc.close();
	}

	public void test(String[] args) {
		/*
		 * args[0] - train/test
		 * args[1] - abcd
		 * args[2] - inputPath;outputPath;SVM_Classification;modelPath;LabelIndex;FeaturesIndexes;
		 */
		String ipPath=null;
		String modelPath = null;
		int labelIndex = 0;
		String resPath = null;
		int nClass = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			handleException("SVMClassification: Insufficient number of arguments");
			return;
		}
		
		try{
			String [] params =args[2].split(";");
			ipPath=params[0];
			labelIndex=convertToInt(params[4]);
			String[] featureStrList = params[5].split(",");
			for(int i=0;i<featureStrList.length;i++){
				featureList.add(convertToInt(featureStrList[i]));
			}
			resPath  = params[1];
			modelPath=params[3];
		}catch(Exception e){
			handleException("SVMClassification: Exception while processing arguments "+ e.getMessage());
			return;
		}
		
		//loading java spark context
		JavaSparkContext sc = getSparkContext("SVMClassification Testing");
		SparkConf conf = new SparkConf().setAppName("SVMClassification Metrics");
		String tIF = null;
		String mID = "test";
		String tmID[] = modelPath.split("/");
		mID = tmID[tmID.length-1];
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
	    final SVMModel savedModel = SVMModel.load(sc.sc(), modelPath);
		/*Details of the Model*/
	 	logger.info("Intercept: " + savedModel.intercept());		
	 	tIF = "classifier\n" + "Intercept: " + savedModel.intercept()+ "\n";
		sumModel = savedModel.toString();
	 	logger.info("Model Summary: " + sumModel);		
	 	tIF = tIF + "Model Summary: " + sumModel + "\n";
	 	
		JavaRDD<String> results = testingDataLabeled.map(
	   			 new Function<LabeledPoint, String>() {
				    	 public String call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				          return point.features().toString() +  " , "+ Double.toString(point.label()) + " , "+ Double.toString(prediction) ;
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
	 	tIF = tIF + "No. of Classes are: "+ nClass + "\n";
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
					tIF = tIF + "Area under ROC curve: " + bcmetrics.areaUnderROC() + "\n";
				 	tIF = tIF + "Number of Bins: " + bcmetrics.numBins() + "\n";
				    tIF = tIF + "Precision by threshold: " + precision.collect() + "\n";
				    tIF = tIF + "Recall by threshold: " + recall.collect() + "\n";
				    tIF = tIF + "F1 Score by threshold: " + f1Score.collect() + "\n";
				    tIF = tIF + "F2 Score by threshold: " + f2Score.collect() + "\n";
				    tIF = tIF + "Precision-recall curve: " + prc.collect() + "\n";
				    tIF = tIF + "ROC curve: " + roc.collect() + "\n";
				    tIF = tIF + "Area under precision-recall curve: " + bcmetrics.areaUnderPR() + "\n";
				    logger.info("Evaluation Metrics stored in /user/eval/"+mID+"_eval.txt");
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
				tIF = tIF + "Confusion matrix: \n" + confusion.toString() + "\n";
			    tIF = tIF + "Precision: " + metrics.precision() + "\n";
			    tIF = tIF + "Recall: " + metrics.recall() + "\n";
			    tIF = tIF + "F1 Score: " + metrics.fMeasure() + "\n";
			    for (int i = 0; i < metrics.labels().length; i++) {
			        tIF = tIF +"Class "+ metrics.labels()[i]+" precision: "+metrics.precision(metrics.labels()[i])+"\n";
			        tIF = tIF +"Class "+ metrics.labels()[i]+" recall: "+metrics.recall(metrics.labels()[i])+"\n";
			        tIF = tIF +"Class "+ metrics.labels()[i]+" F1 score: "+metrics.fMeasure(metrics.labels()[i])+"\n";
			    }
			    tIF = tIF +"Weighted precision: "+metrics.weightedPrecision()+"\n";
			    tIF = tIF +"Weighted recall: "+ metrics.weightedRecall()+"\n";
			    tIF = tIF +"Weighted F1 score: "+metrics.weightedFMeasure()+"\n" ;
			    tIF = tIF +"Weighted false positive rate: "+metrics.weightedFalsePositiveRate()+"\n";
			    logger.info("Evaluation Metrics stored in /user/eval/"+mID+"_eval.txt");
		}
	    Path newFilePath=new Path("/user/eval"+"/"+mID+"_eval.txt");
	    FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new Configuration());
		    hdfs.createNewFile(newFilePath);
		    FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		    fsOutStream.write(tIF.getBytes());
		    fsOutStream.close();
		} catch (Exception e) {
			logger.error("SVMClassification: Failure in writing to file");
			e.printStackTrace();
		}
		sc.close();
	}

}
