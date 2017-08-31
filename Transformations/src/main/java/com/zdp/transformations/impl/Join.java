package com.zdp.transformations.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Joining two datasets by joining columns input - two input files output -
 * output dataset with set joining column and selected columns
 * 
 * @author 19173
 *
 */

public class Join extends AbstractTransformations implements Serializable {

	private static final long serialVersionUID = 1L;
	// Logger logger = Logger.getLogger(Join.class);
	public String[] args;

	public Join(String[] arg) {
		args = arg;
	}

	public void execute() {

		// args[0] - type
		// args[1] - schema
		// args[2] - inputPath1;inputPath2;
		// outputPath;joinIndexes1;joinIndexes2;
		try {
			String[] params = args[2].split(";");
			// String [] inputPath = params[0].split(",");
			JavaSparkContext sc = new JavaSparkContext();
			// SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
			JavaRDD<String> inputDataRDD = sc.textFile(params[0]);
			final boolean ignoreMetaData = params[0].endsWith("/cleansed");

			// create join index columns
			String[] joinIndexes1 = params[3].split(",");
			final int[] joinIndex1 = new int[joinIndexes1.length];
			for (int i = 0; i < joinIndexes1.length; i++) {
				joinIndex1[i] = Integer.parseInt(joinIndexes1[i]);
			}
			// create a keyvalue pair with joining columns as key, remaining
			// records as value
			JavaPairRDD<String, String> keyValuePair1 = inputDataRDD
					.mapToPair(new PairFunction<String, String, String>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<String, String> call(String line) {
							String[] parts = line.split(",");
							String key = "";

							for (int i = 0; i < joinIndex1.length; i++) {
								key = key + parts[joinIndex1[i]] + ",";
							}
							/**
							 * This is done to take care of emitting last 2
							 * columns(timestamp and fileSource name) in
							 * Ingested datasets. Only datasets that are
							 * ingested will have these columns.
							 */

							if (ignoreMetaData) {
								line = line.substring(0, line.lastIndexOf(","));
								line = line.substring(0, line.lastIndexOf(","));
							}
							return new Tuple2<String, String>(key.substring(0, key.length() - 1), line);
						}
					});
			// create joinIndex columns
			String[] joinIndexes2 = params[4].split(",");
			final int[] joinIndex2 = new int[joinIndexes2.length];
			for (int i = 0; i < joinIndexes2.length; i++) {
				joinIndex2[i] = Integer.parseInt(joinIndexes2[i]);
			}

			JavaRDD<String> inputDataRDD1 = sc.textFile(params[1]);
			final boolean ignoreMetaData2 = params[0].endsWith("/cleansed");
			// create a keyvalue pair with joining columns as key, remaining
			// records as value
			JavaPairRDD<String, String> keyValuePair2 = inputDataRDD1
					.mapToPair(new PairFunction<String, String, String>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<String, String> call(String line) {
							String[] parts = line.split(",");
							String key = "";
							// String key = parts[joinIndex2];
							for (int i = 0; i < joinIndex2.length; i++) {
								key = key + parts[joinIndex2[i]] + ",";
							}

							// removing key from the second dataset
							String tmp = "";
							for (int i = 0; i < parts.length; i++) {
								// if(i!=joinIndex2[0])
								for (int j = 0; j < joinIndex2.length; j++) {
									if (i != joinIndex2[j]) {
										tmp = tmp + parts[i] + ",";
									}
								}
							}
							if (!tmp.isEmpty()) {
								tmp = tmp.substring(0, tmp.length() - 1);
							}
							/**
							 * This is done to take care of emitting last 2
							 * columns(timestamp and fileSource name) in
							 * Ingested datasets. Only datasets that are
							 * ingested will have these columns.
							 */
							if (ignoreMetaData2) {
								tmp = tmp.substring(0, tmp.lastIndexOf(","));
								tmp = tmp.substring(0, tmp.lastIndexOf(","));
							}

							return new Tuple2<String, String>(key.substring(0, key.length() - 1), tmp);
						}
					});
			// join the data with joining columns
			JavaPairRDD<String, Tuple2<String, String>> joinedResult = keyValuePair1.join(keyValuePair2);

			// modify the data to save results in hdfs.
			JavaRDD<String> finalresult = joinedResult
					.map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
						private static final long serialVersionUID = 1L;

						public String call(Tuple2<String, Tuple2<String, String>> s) {
							String s1 = s._2._1 + "," + s._2._2;
							return s1;
						}
					});
			finalresult.repartition(1).saveAsTextFile(params[2]);
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
