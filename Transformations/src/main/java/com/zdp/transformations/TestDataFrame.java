package com.zdp.transformations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SQLContext;

import com.zdp.transformations.functions.ReplaceWithCustomValue;

import scala.Tuple2;

public class TestDataFrame {

	public static void main(String[] args) {

		/*
		 * args[0] = the Type of Transformation args[1] = The schema of the data
		 * set where feilds separated by "," and the fields and it's data type
		 * are separated by ":" args[2] = <source Folder Location>:<destination
		 * Folder Location>:<subtype of the transformation>:<column
		 * indexes>:<value to be replaced as applicable>
		 */
		String schema = "name:String,age:int,salary:double";
		String customval = "99";
		String indexes = "1,2";
		String destPath = "hdfs://Zlab-physrv1:8020/user/nisith/median/";
		final String[] indexArr = indexes.split(",");
		List<String> columns = new ArrayList<String>();
		SparkContext sc = new SparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> jrdd = jsc.textFile("hdfs://Zlab-physrv1:8020/user/nisith/data/TestEmp.txt");
		// Generate the schema based on the string of schema
		String[] schemSplit = schema.split(",");
		String[] indSplit = indexes.split(",");
		Map<String, String> schemaMap = new HashMap<String, String>();
		for (int i = 0; i < schemSplit.length; i++) {
			String[] temp = schemSplit[i].split(":");
			schemaMap.put(new Integer(i).toString(), temp[1]);
		}

		// Convert records of the RDD (people) to Rows.
		/*
		 * JavaRDD<Row> rowRDD = jrdd.map(new Function<String, Row>() { public
		 * Row call(String record) throws Exception { String[] fields =
		 * record.split(",", -1); return RowFactory.create((Object[]) fields); }
		 * });
		 */

		// Apply the schema to the RDD.
		/*
		 * DataFrame df = sqlContext.createDataFrame(rowRDD, struct);
		 * df.registerTempTable("TempRDDTable");
		 */

		/*
		 * String tempSql = "select "; for (int i = 0; i < columns.size(); i++)
		 * { if (Arrays.asList(indexArr).contains(new Integer(i).toString()) &&
		 * i != (columns.size() - 1)) { tempSql = tempSql + " if(" +
		 * columns.get(i) + ", NULL, " + customval + ") as " + columns.get(i) +
		 * ","; } else if (Arrays.asList(indexArr).contains(new
		 * Integer(i).toString())) { tempSql = tempSql + " if(" + columns.get(i)
		 * + ", NULL, " + customval + ") as " + columns.get(i) + " "; } else if
		 * (i != (columns.size() - 1)) { tempSql = tempSql + columns.get(i) +
		 * ", "; } else { tempSql = tempSql + columns.get(i) + " "; } } tempSql
		 * = tempSql + "from TempRDDTable;"; System.out.println("Sql : "
		 * +tempSql); df.sqlContext().sql(tempSql);
		 */
		// df.select(df.col("age").when(df.col("age").$greater("30"),"99")).show();

		/*
		 * JavaRDD<Vector> parsedData = jrdd.map(new Function<String, Vector>()
		 * { public Vector call(String s) { String[] val = s.split(",", -1);
		 * List<Double> tmp = new ArrayList<Double>();
		 * 
		 * for (String in : indexArr) { int i = new Integer(in); if (null ==
		 * val[i] || val[i].isEmpty()) { tmp.add(Double.parseDouble("0")); }
		 * else { tmp.add(Double.parseDouble(val[i].trim())); } } double x[] =
		 * new double[tmp.size()]; for (int i = 0; i < x.length; i++) { x[i] =
		 * tmp.get(i); } System.out.println(Vectors.dense(x)); return
		 * Vectors.dense(x); } });
		 * 
		 * MultivariateStatisticalSummary stats =
		 * Statistics.colStats(parsedData.rdd());
		 * 
		 * System.out.println("Calculating mean ===");
		 * System.out.println(stats.mean()); Vector mean = stats.mean();
		 * System.out.println(mean);
		 */

		/*
		 * Map<String, Tuple2<Double, Long>> Total = pairs .reduceByKey(new
		 * Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double,
		 * Long>>() { public Tuple2<Double, Long> call(Tuple2<Double, Long> a,
		 * Tuple2<Double, Long> b) { Double d1 = a._1(); Double d2 = b._1();
		 * Long l1 = a._2(); Long l2 = b._2();
		 * 
		 * return new Tuple2<Double, Long>(d1 + d2, l1 + l2); }
		 * }).collectAsMap();
		 * 
		 * for(String s:Total.keySet()){ System.out.println("Key :"+s);
		 * System.out.println("Total :"+Total.get(s)._1()); System.out.println(
		 * "count :"+Total.get(s)._2()); System.out.println("Avg Or Mean:"
		 * +((Total.get(s)._1())/(Total.get(s)._2()))); }
		 */

		System.out.println("************** Filter Process Started ***************");
		JavaRDD<Tuple2<Tuple2<String, Double>, String>> filteredRDD = jrdd
				.flatMap(new FlatMapFunction<String, Tuple2<Tuple2<String, Double>, String>>() {
					public Iterable<Tuple2<Tuple2<String, Double>, String>> call(String s) {
						List<Tuple2<Tuple2<String, Double>, String>> list = new ArrayList<Tuple2<Tuple2<String, Double>, String>>();
						String[] val = s.split(",", -1);
						for (String in : indexArr) {
							int i = new Integer(in);
							if (!(null == val[i] || val[i].isEmpty())) {
								list.add(new Tuple2<Tuple2<String, Double>, String>(
										new Tuple2<String, Double>(in, Double.parseDouble(val[i].trim())), null));
							}
						}
						return list;
					}
				});
		System.out.println("************** Filter Process Started ***************");
		Map<String, Integer> indxPartnMap = new HashMap<String, Integer>();
		int[] partitionIds = new int[indexArr.length];
		for (int i = 0; i < indexArr.length; i++) {
			indxPartnMap.put(indexArr[i], i);
			partitionIds[i] = i;
		}
		JavaPairRDD<Tuple2<String, Double>, String> pairs = JavaPairRDD.fromJavaRDD(filteredRDD)
				.repartitionAndSortWithinPartitions(new PartitionByColumn(indexArr.length, indxPartnMap), null);
		Map<String, String> valuesToIndex = new HashMap<String, String>();
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
				System.out.println("Index : " + key + "  ------ Median : " + custVal + " ----Size: " + partitions.size()
						+ " -----Pos: " + (partitions.size() / 2));

			} else {
				valuesToIndex.put(key, "0");
			}

		}
		jrdd.map(new ReplaceWithCustomValue(valuesToIndex, indSplit)).saveAsTextFile(destPath);

	}

}

class PartitionByColumn extends Partitioner {

	private int numParts;
	private Map<String, Integer> indxPartnMap;

	public PartitionByColumn() {

	}

	public PartitionByColumn(int numParts, Map<String, Integer> indxPartnMap) {
		this.numParts = numParts;
		this.indxPartnMap = indxPartnMap;
	}

	@Override
	public int numPartitions() {
		return numParts;
	}

	public int getPartition(Object key) {
		Tuple2<String, Double> tuple = (Tuple2<String, Double>) key;
		return indxPartnMap.get(tuple._1());
	}
}
