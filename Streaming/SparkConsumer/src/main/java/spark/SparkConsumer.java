package spark;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SparkConsumer {
	public static Logger LOG = Logger.getLogger(SparkConsumer.class);

	private static String KAFKA_BROKER;
	private static String KAFKA_PORT;
	private static String GROUPID;
	private static String TOPIC;
	private static String OUTPUT_LOCATION;
	private static int BATCH_DURATION;
	private static String MASTER;
	private static String APP_NAME;
	private static String USER;
	private static String FQCN;
	private static String TRANMETHOD;
	private static String JARLOCATION;
	private static String CONNAME;
	static boolean dotransflag;

	public static void main(String args[]) {

		LOG.debug("SparkConsumer main method with arguments " + Arrays.toString(args));
		if (args == null || args.length < 15) {
			System.err.println("The number of arguments required is minimum 5");
			System.err.println("Arguments to the function should be as follow\n"
					+ "Argument 1: Kafka Broker such as ec2-54-174-149-226.compute-1.amazonaws.com\n"
					+ "Argument 2: Zookeeper Port such as 2181\n"
					+ "Argument 3: Consumer Group such as IngestionConsumer\n" + "Argument 4: Topic such as test\n"
					+ "Argument 5: username of user who created profile, data will be stored in hdfs as /user/zeas/stream/\n"
					+ "Argument 6: Batch Duration in Seconds\n" + "Argument 7: Location of transformation jar\n"
					+ "Argument 8: Class of transformation jar\n" + "Argument 9: Transformation Method\n"

			);
			System.exit(-1);
		}
		System.out.println("ARGUMENTS length " + args.length);
		KAFKA_BROKER = getArgumentValue(args, "--broker");
		System.out.println("Broker " + KAFKA_BROKER);
		KAFKA_PORT = getArgumentValue(args, "--port");
		System.out.println("Broker port " + KAFKA_PORT);
		GROUPID = getArgumentValue(args, "--group");
		System.out.println("Group " + GROUPID);
		TOPIC = getArgumentValue(args, "--topic");
		System.out.println("Topic " + TOPIC);
		USER = getArgumentValue(args, "--user");
		System.out.println(USER);
		CONNAME = getArgumentValue(args, "--conname");
		System.out.println(CONNAME);
		System.out.println("batch duration " + getArgumentValue(args, "--duration"));
		BATCH_DURATION = Integer.parseInt(getArgumentValue(args, "--duration"));
		// If transformation rule is applicable
		if (args.length == 21) {
			FQCN = getArgumentValue(args, "--fqcn");
			System.out.println("Class name " + FQCN);
			TRANMETHOD = getArgumentValue(args, "--tranmthd");
			System.out.println("Method Name " + TRANMETHOD);
			JARLOCATION = getArgumentValue(args, "--jar");
			System.out.println("Jar Location " + JARLOCATION);
			dotransflag = true;
		}

		OUTPUT_LOCATION = "/user/zeas/stream/" + USER + "/" + CONNAME;
		System.out.println("Output Location " + OUTPUT_LOCATION);
		MASTER = "local[*]";
		APP_NAME = "ZEAS STREAMING-" + CONNAME;

		SparkConf conf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
		final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(BATCH_DURATION));

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(TOPIC, 1);

		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc,
				KAFKA_BROKER + ":" + KAFKA_PORT, GROUPID, topicMap);

		LOG.debug("Kafka Stream created using \n" + KAFKA_PORT + ":" + KAFKA_PORT + " \ntopic= " + topicMap.toString()
				+ "\nGroupId " + GROUPID);

		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		System.out.println("Transformation is to be done " + dotransflag);

		// lines.print();
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) throws Exception {
				LOG.debug("Printing to file " + OUTPUT_LOCATION + " " + rdd.toString());
				// System.out.println("Printing to file " + OUTPUT_LOCATION + "
				// " + rdd.collect());
				// call transformation method here

				if (dotransflag) {
					String transformedData = doTransformation(rdd.collect(), JARLOCATION, FQCN, TRANMETHOD);
					if (transformedData != null && !transformedData.equals("null")
							&& Arrays.asList(transformedData).size() > 0) {

						writeToHadoop(Arrays.asList(transformedData), jssc.sparkContext());
					}
				} else {
					// if no transformation is done
					writeToHadoop(rdd.collect(), jssc.sparkContext());
				}

			}

			private void writeToHadoop(List<String> transformedData, JavaSparkContext jsc) {
				// TODO Auto-generated method stub

				LOG.info("Writing data to HDFS " + transformedData);
				System.out.println(transformedData.size() + " SIZE ");
				if (transformedData == null || transformedData.size() == 0)
					return;
				JavaRDD<String> rdd = jsc.parallelize(transformedData);
				JavaRDD<String> colRdd = rdd.coalesce(1);
				System.out.println("Writing data to HDFS " + transformedData + " " + OUTPUT_LOCATION);

				colRdd.saveAsTextFile(OUTPUT_LOCATION + "/" + System.currentTimeMillis() + "/");

			}
		});
		jssc.start();
		jssc.awaitTermination();

	}

	private static String getArgumentValue(String args[], String name) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(name)) {
				if (args.length >= i + 1) {
					return args[i + 1];
				} else {
					LOG.error("No value found for argument " + name);
				}
			}
		}
		LOG.error("No argument found " + name);
		return null;
	}

	/**
	 * 
	 * @param args
	 * 
	 *            args[0] is location of Transformation jar args[1] is name of
	 *            Transformation class args[2] is name of method which takes
	 *            messages and transforms it and returns
	 * 
	 * @return
	 * @throws MalformedURLException
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	/*
	 * static String msg =
	 * "17/05/29 14:52:47 TRACE ServletInvocableHandlerMethod: Method [com.itc.zeas.profile.ProfileController.getprofileRunStatus] returned [<200 OK,{13391=Completed, 13421=New, 13387=Completed, 13656=Completed, 13691=New, 13383=New, 13652=Completed, 13687=Completed, 13415=Completed, 13759=New, 13379=Completed, 13683=Completed, 13411=Completed, 13696=Started, 13403=Completed, 13375=New},{}>]"
	 * ;
	 * 
	 * public static void main(String[] args) throws MalformedURLException,
	 * ClassNotFoundException, NoSuchMethodException, SecurityException,
	 * IllegalAccessException, IllegalArgumentException,
	 * InvocationTargetException { doTransformation(Arrays.asList(msg),
	 * "D:/jArS/ZEASLog.jar","transformationRule.ZEASTransformationRule",
	 * "transformedRDD"); }
	 */

	private static String doTransformation(List<String> msgs, String... args) {
		try {
			URL jarLocation = new URL("file:///" + args[0]);
			String FQCN = args[1];// transformationRule.ZEASTransformationRule
			String methodName = args[2];// transformedRDD
			// System.out.println("Doing transformation on " + msgs);
			System.out.println("JAr " + jarLocation + " FQCN " + FQCN + " method " + methodName);
			URLClassLoader classLoader = new URLClassLoader(new URL[] { jarLocation });
			Class transClass = Class.forName(FQCN, true, classLoader);

			Method transMethod = transClass.getMethod(methodName, List.class);
			String transformedData = (String) transMethod.invoke(transClass, msgs);
			System.out.println(transformedData);

			return transformedData;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

}
