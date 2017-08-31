package com.zeas.spark.mlib.algos;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.zeas.spark.mlib.functions.GetLabeledData;
public class NaiveBayesClassification  extends AbstractMLAlgo implements Serializable {

	private static Logger logger = Logger.getLogger(NaiveBayesClassification.class);
	public static void main(String[] args)
	{
		if(args[0].equals("train"))
			new NaiveBayesClassification().train(args);
		else if(args[0].equals("test"))
			new NaiveBayesClassification().test(args);
	}
	public void train(String[] args) {

	/*
	 * args[0] - train/test
	 * args[1] - schema
	 * args[2] - inputPath;outputPath;Naive_Bayes_Classification;labelIndex;FeaturesIndex;lambda
	 * @see com.zeas.spark.mlib.iface.IMLAlgorithm#train(java.lang.String[])
	 */
	ArrayList<Integer> featureList=new ArrayList<Integer>();
	String hdfsPath=null;
	double lambda = 1.0;
	String modelSaveLocation = null;
	int labelIndex = 0;
	if(args.length<3){
		handleException("NaiveBayesClassification: Insufficient number of arguments");
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
		lambda = convertToDouble(params[5]);
	}catch(Exception e){
		handleException("NaiveBayesClassificationTraining: Exception while processing arguments "+ e.getMessage());
	}
	JavaSparkContext sc = getSparkContext("NaiveBayesClassificationTraining");

	JavaRDD<String> trainingData = sc.textFile(hdfsPath);
	JavaRDD<LabeledPoint> trainingDataLabeled = trainingData.map(new GetLabeledData(labelIndex, featureList));
	//JavaRDD<LabeledPoint> trainingDataLabeled = getLabeledData(trainingData, labelIndex, featureList, delimiter);
	trainingDataLabeled.cache();

	final NaiveBayesModel model = NaiveBayes.train(JavaRDD.toRDD(trainingDataLabeled), lambda);
	model.save(sc.sc(), modelSaveLocation);
	logger.info("Model saved at : "+ modelSaveLocation);
	sc.close();

	}

	
	public void test(String[] args) {
		/*
		 * args[0] - test
		 * args[1] - schema
		 * args[2] - inputPath;outputPath;NaiveBayesClassification;modelPath;labelIndex;FeaturesIndex;
		 */
		JavaSparkContext sc = getSparkContext("NaiveBayesClassification Testing");
		SparkConf conf = new SparkConf().setAppName("NaiveBayesClassification Metrics");
		String modelPath=null;
		String errorMessage = null;
		String ipPath=null;
		String resPath = null;
		int nClass = 0;
		//String modelSaveLocation = null;
		int labelIndex = 0;
		ArrayList<Integer> featureList=new ArrayList<Integer>();
		if(args.length<3){
			errorMessage = "NaiveBayesClassification: Insufficient number of arguments";
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
			errorMessage = "NaiveBayesClassification: Exception while processing arguments "+ e.getMessage();
			logger.error(errorMessage);
			return;
		}
		String sumModel = null;
		String tIF = null;
		String mID = "test";
		String tmID[] = modelPath.split("/");
		mID = tmID[tmID.length-1];
		JavaRDD<String> testData = sc.textFile(ipPath);
		final int label = labelIndex;	
		//testing the data
		JavaRDD<LabeledPoint> testingDataLabeled = testData.map(new GetLabeledData(labelIndex, featureList));
		//getting the details of saved model
	    final NaiveBayesModel savedModel = NaiveBayesModel.load(sc.sc(),modelPath);
		testingDataLabeled.cache();
	    //creating RDD to get details- label,features,predictions
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
		JavaRDD<Integer> labelData = testData.map(new Function<String,Integer>(){
			public Integer call(String line)  {
				String[] parts = line.split(",");
					return Integer.parseInt(parts[label]);							
			}
			  });
		JavaRDD<Tuple2<Object,Object>> valueAndPred = testingDataLabeled.map(
				 new Function<LabeledPoint, Tuple2<Object,Object>>() {
				    	 public Tuple2<Object,Object> call(LabeledPoint point) {
				         double prediction = savedModel.predict(point.features());
				         return new Tuple2<Object,Object>(prediction,point.label());
				        }
				      }
				    );
		JavaPairRDD<Double, Double> predictionAndLabel = 
	    		testingDataLabeled.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					public Tuple2<Double, Double> call(LabeledPoint  p) throws Exception {
				         double prediction = savedModel.predict(p.features());
				         return new Tuple2<Double, Double>(prediction, p.label());
					}
	    		  });
	    		double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
					public Boolean call(Tuple2<Double, Double> pl)
							throws Exception {
		    		      return pl._1().equals(pl._2());
					}
	    		  }).count() / (double) testingDataLabeled.count();

	    	    logger.info("Accuracy: " + accuracy);
	    	    tIF = "classifier\n" +"Accuracy: " + accuracy + "\n";
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
	    	 	tIF = tIF + "No. of Classes are : "+ nClass + "\n";
	    		sumModel = savedModel.toString();
	    	 	logger.info("Model Summary: " + sumModel);		
	    	 	tIF = tIF + "Model Summary: " + sumModel + "\n";
	    	 	if(nClass==2)//binary classification 
				{
						//Evaluation Metrics (Binary Classification)
					 	BinaryClassificationMetrics bcmetrics = new BinaryClassificationMetrics(valueAndPred.rdd());		
					 	logger.info("Area under ROC curve: " + bcmetrics.areaUnderROC());
					 	logger.info("Number of Bins: " + bcmetrics.numBins());
					 
					 	//Experiment with the next line
//					 	JavaRDD<Tuple2<Object,Object>>scoreAndLabels = bcmetrics.scoreAndLabels().toJavaRDD();
					 
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
	    			logger.error("NaiveBayesClassification: Failure in writing to file");
	    			e.printStackTrace();
	    		}
	    		sc.close();
	}
}