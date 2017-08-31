package com.zdp.transformations.impl;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.zdp.transformations.functions.CustomRDDPartioner;
import com.zdp.transformations.functions.FilterRequiredColumnsForMean;
import com.zdp.transformations.functions.FilterRequiredColumnsForMedian;
import com.zdp.transformations.functions.GetMeanForCols;
import com.zdp.transformations.functions.RemoveEmptyRowsForColumns;
import com.zdp.transformations.functions.ReplaceWithCustomValue;

import scala.Tuple2;

/**
 * This Class is Responsible for Replacing the empty columns with custom value
 * or Removing the rows having no values in the given column
 * 
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class ReplaceOrRemoveMissingData extends AbstractTransformations {

	/*
	 * schema = The schema of the data set.
	 * 
	 * customVal = Custom Value to be replaced.
	 * 
	 * indexes = index of the column in which the new values will be replaced or
	 * if null the row will be deleted.
	 * 
	 * sourcePath = The Source folder location.
	 * 
	 * destPath = Destination folder location.
	 * 
	 * type = Type of transformation to be applied.
	 */

	private Logger logger = Logger.getLogger(ReplaceOrRemoveMissingData.class);
	private String schema;
	private String customVal;
	private String indexes;
	private String sourcePath;
	private String destPath;
	private String type;

	public ReplaceOrRemoveMissingData(String schema, String customval, String indexes, String sourcePath,
			String destPath, String type) {
		this.customVal = customval;
		this.schema = schema;
		this.indexes = indexes;
		this.sourcePath = sourcePath;
		this.destPath = destPath;
		this.type = type;
	}

	public ReplaceOrRemoveMissingData(String[] args) {
		// args[0] - type of transformation
		// args[1] - schema
		// args[2] - InputPath ; OutputPath ;column indexes;operation;custom
		// value

		this.schema = args[1];

		String vals[] = args[2].split(";");
		System.out.println(" vals : " + args[2] + "   --- size" + vals.length);
		this.sourcePath = vals[0];
		this.destPath = vals[1];
		this.indexes = vals[2];
		this.type = vals[3];
		if (vals[3].equalsIgnoreCase("Custom_Value")) {
			this.customVal = vals[4];
		}
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getCustomVal() {
		return customVal;
	}

	public void setCustomVal(String customVal) {
		this.customVal = customVal;
	}

	public String getIndexes() {
		return indexes;
	}

	public void setIndexes(String indexes) {
		this.indexes = indexes;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getDestPath() {
		return destPath;
	}

	public void setDestPath(String destPath) {
		this.destPath = destPath;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void execute() {

		logger.info("------- Execute Method Started");
		SparkContext sc = new SparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		// SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> jrdd = jsc.textFile(sourcePath);
		// Generate the schema based on the string of schema
		Map<String, String> valuesToIndex = null;
		String[] schemSplit = this.schema.split(",");
		String[] indSplit = this.indexes.split(",");
		Map<String, String> schemaMap = new HashMap<String, String>();
		for (int i = 0; i < schemSplit.length; i++) {
			String[] temp = schemSplit[i].split(":");
			schemaMap.put(new Integer(i).toString(), temp[1]);
		}

		if (type.equalsIgnoreCase("Custom_Value")) {
			logger.info("------- Custom Value Replacement Started");
			valuesToIndex = new HashMap<String, String>();
			for (String in : indSplit) {
				valuesToIndex.put(in, this.customVal);
			}
			jrdd.map(new ReplaceWithCustomValue(valuesToIndex, indSplit)).saveAsTextFile(destPath);
			logger.info("------- Custom Value Replacement Finsihed");
		} else if (type.equalsIgnoreCase("Replace_With_mean")) {
			logger.info("------- Replace With mean Replacement Started");
			JavaRDD<Tuple2<String, Tuple2<Double, Long>>> filteredRDD = jrdd
					.flatMap(new FilterRequiredColumnsForMean(indSplit));
			JavaPairRDD<String, Tuple2<Double, Long>> pairs = JavaPairRDD.fromJavaRDD(filteredRDD);
			Map<String, Tuple2<Double, Long>> means = pairs.reduceByKey(new GetMeanForCols()).collectAsMap();
			valuesToIndex = new HashMap<String, String>();
			for (String key : means.keySet()) {
				System.out.println("Key :" + key);
				System.out.println("Total :" + means.get(key)._1());
				System.out.println("count :" + means.get(key)._2());
				Double mean = (means.get(key)._1()) / (means.get(key)._2());
				System.out.println("Avg Or Mean:" + ((means.get(key)._1()) / (means.get(key)._2())));
				if (schemaMap.get(key).equalsIgnoreCase("int") || schemaMap.get(key).equalsIgnoreCase("long")) {
					valuesToIndex.put(key, new Long(Math.round(mean)).toString());
				} else {
					valuesToIndex.put(key, mean.toString());
				}
			}
			jrdd.map(new ReplaceWithCustomValue(valuesToIndex, indSplit)).saveAsTextFile(destPath);
			logger.info("------- Replace With mean Replacement Finished");
		} else if (type.equalsIgnoreCase("Replace_With_median")) {
			logger.info("------- Replace With median Replacement Started");
			JavaRDD<Tuple2<Tuple2<String, Double>, String>> filteredRDD = jrdd
					.flatMap(new FilterRequiredColumnsForMedian(indSplit));
			Map<String, Integer> indxPartnMap = new HashMap<String, Integer>();
			for (int i = 0; i < indSplit.length; i++) {
				indxPartnMap.put(indSplit[i], i);
			}
			JavaPairRDD<Tuple2<String, Double>, String> pairs = JavaPairRDD.fromJavaRDD(filteredRDD)
					.repartitionAndSortWithinPartitions(new CustomRDDPartioner(indSplit.length, indxPartnMap),
							new CompareTuple());
			valuesToIndex = new HashMap<String, String>();
			List<Tuple2<Tuple2<String, Double>, String>> partitions;
			for (String key : indxPartnMap.keySet()) {
				partitions = pairs.collectPartitions(new int[] { indxPartnMap.get(key) })[0];
				if (!(partitions.isEmpty())) {
					Double custVal;
					if ((partitions.size() % 2) == 0) {
						custVal = (partitions.get(partitions.size() / 2)._1._2()
								+ partitions.get((partitions.size() / 2) - 1)._1._2()) / 2;
					} else {
						custVal = partitions.get((partitions.size()) / 2)._1._2();
					}
					if (schemaMap.get(key).equalsIgnoreCase("int") || schemaMap.get(key).equalsIgnoreCase("long")) {
						valuesToIndex.put(key, new Long(Math.round(custVal)).toString());
					} else {
						valuesToIndex.put(key, custVal.toString());
					}
					System.out.println("Index : " + key + "  ------ Median : " + custVal + " ----Size: "
							+ partitions.size() + " -----Pos: " + (partitions.size() / 2));

				} else {
					valuesToIndex.put(key, "0");
				}

			}
			jrdd.map(new ReplaceWithCustomValue(valuesToIndex, indSplit)).saveAsTextFile(destPath);
			logger.info("------- Replace With mean Replacement Finished");
		} else if (type.equalsIgnoreCase("Remove_entire_row")) {
			logger.info("------- Remove entire row Started");
			jrdd.map(new RemoveEmptyRowsForColumns(indSplit)).filter(new RemoveIt()).saveAsTextFile(destPath);
			logger.info("------- Remove entire row Finished");
		}
		jsc.close();
		logger.info("------- Execute Method Finished");
	}

}

class RemoveIt implements Function<String, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8818661291811285167L;

	public Boolean call(String arg0) throws Exception {
		Boolean val = true;
		if (arg0.equalsIgnoreCase("RemoveIt")) {
			val = false;
		}
		return val;
	}
}

class CompareTuple implements Serializable, Comparator<Tuple2<String, Double>> {

	private static final long serialVersionUID = 8138975065874944500L;

	public int compare(Tuple2<String, Double> tuple1, Tuple2<String, Double> tuple2) {

		return tuple1._2.compareTo(tuple2._2);
	}
}
