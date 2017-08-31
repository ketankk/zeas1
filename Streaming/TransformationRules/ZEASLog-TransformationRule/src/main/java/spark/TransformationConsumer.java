package spark;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;
import transformationRule.ZEASTransformationRule;
import spark.TransformProducer;

public class TransformationConsumer {
	public static Logger LOG = Logger.getLogger(TransformationConsumer.class);

	private static final Pattern COMMA = Pattern.compile(",");

	private static String KAFKA_BROKER;
	private static String KAFKA_PORT;
	private static String GROUPID;
	private static String TOPIC;
	private static String OUTPUT_LOCATION;
	private static int BATCH_DURATION;
	private static String MASTER;
	private static String APP_NAME;
	private static String USER;
	private static String TARGET_TOPIC;

	@SuppressWarnings("serial")
	public static void main(String args[]) {

		LOG.debug("SparkConsumer main method with arguments " + Arrays.toString(args));
		if (args == null || args.length < 12) {
			System.err.println("The number of arguments required is minimum 5");
			System.err.println("Arguments to the function should be as follow\n"
					+ "Argument 1: Kafka Broker such as ec2-54-174-149-226.compute-1.amazonaws.com\n"
					+ "Argument 2: Zookeeper Port such as 2181\n"
					+ "Argument 3: Consumer Group such as IngestionConsumer\n" + "Argument 4: Topic such as test\n"
					+ "Argument 5: username of user who created profile, data will be stored in hdfs as /user/zeas/stream/\n"
					+ "Argument 6: Batch Duration in Seconds\n");
			System.exit(-1);
		}

		KAFKA_BROKER = getArgumentValue(args, "--broker");
		System.out.println("Broker " + KAFKA_BROKER);

		KAFKA_PORT = getArgumentValue(args, "--port");
		System.out.println("Broker port " + KAFKA_PORT);

		GROUPID = getArgumentValue(args, "--group");
		System.out.println("Group " + GROUPID);

		TOPIC = getArgumentValue(args, "--topic");
		System.out.println("Topic " + TOPIC);

		USER = getArgumentValue(args, "--user");

		OUTPUT_LOCATION = "/user/zeas/stream/" + USER;
		System.out.println("Ouput Location " + OUTPUT_LOCATION);

		BATCH_DURATION = Integer.parseInt(getArgumentValue(args, "--duration"));
		System.out.println("batch duration " + getArgumentValue(args, "--duration"));

		MASTER = "local[*]";
		APP_NAME = "ZEAS STREAMING-" + USER;

		TARGET_TOPIC = getArgumentValue(args, "--targetTopic");

		SparkConf conf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(BATCH_DURATION));

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(TOPIC, 1);

		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc,
				KAFKA_BROKER + ":" + KAFKA_PORT, GROUPID, topicMap);

		LOG.debug("Kafka Stream created using \n" + KAFKA_PORT + ":" + KAFKA_PORT + " \ntopic= " + topicMap.toString()
				+ "\nGroupId " + GROUPID);
		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				// System.out.println("1st "+tuple2._1+" 2nd is "+tuple2._2);
				return tuple2._2;
			}
		});

		lines.print();

		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) throws Exception {

				// System.out.println(rdd.collect() + "%%%%%%%");

				// Parsing logic here
				if (rdd.collect().size() > 0) {
					rdd.saveAsTextFile(OUTPUT_LOCATION + "/" + GROUPID + "/" + System.currentTimeMillis() + "/");

					Map<String, Map<Integer, String>> transformedData = ZEASTransformationRule
							.readConsumerMessage(rdd.collect());
					if (transformedData.size() > 0) {
						System.out.println("#############" + transformedData.toString() + "*******************");
						// Call Producer to write into Kafka
						if (transformedData.size() > 0) {
							String finalOutput=getMessageString(transformedData);
							saveInHadoop(finalOutput);
							System.out.println("Producing "+finalOutput+" to "+TARGET_TOPIC);
							TransformProducer producer = new TransformProducer(TARGET_TOPIC, true,finalOutput
									);
							producer.start();
						}
					}
				}
				LOG.info("No data available for writing");

			}

			private void saveInHadoop(String finalOutput) {

				
				
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

	public static String getMessageString(Map<String, Map<Integer, String>> map) {

		StringBuilder builder = new StringBuilder();

		for (Entry<String, Map<Integer, String>> entry : map.entrySet()) {
			String timeStamp = entry.getKey();

			Map<Integer, String> value = entry.getValue();

			for (Entry<Integer, String> entry1 : value.entrySet()) {

				Integer jobID = entry1.getKey();
				String jobStatus = entry1.getValue();

				builder.append(timeStamp + "," + jobID + "," + jobStatus+"\n");
				//System.out.println(builder.toString());

			}
			//separate each entry by line break
			//builder.append("\n");

		}
		//System.out.println(builder.toString());
		return builder.toString();
	}

}
