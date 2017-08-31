package com.zdp.stream.consumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class StreamConsumer {

	private static final Pattern COMMA = Pattern.compile(",");

	private static String KAFKA_BROKER;
	private static String KAFKA_PORT;
	private static String GROUPID;
	private static String TOPIC;
	private static String OUTPUT_LOCATION;
	private static int BATCH_DURATION;
	private static String MASTER;
	private static String APP_NAME;



	/*Arguments to the function should be as follow
	 * Argument 1: Kafka Broker such as ec2-54-174-149-226.compute-1.amazonaws.com
	 * Argument 2: Kafka Port such as 2181
	 * Argument 3: Consumer Group such as IngestionConsumer
	 * Argument 4: Topic such as test
	 * Argument 5: Output HDFS location such as hdfs:/user/19491/iotdata/
	 * Argument 6: Batch Duration in Seconds
	 */

	public static void main(String args[]){
		if(args==null || args.length<6){
			System.err.println("The number of arguments required is minimum 5");
			System.err.println("Arguments to the function should be as follow\n"+
					"Argument 1: Kafka Broker such as ec2-54-174-149-226.compute-1.amazonaws.com\n"+
					"Argument 2: Kafka Port such as 2181\n"+
					"Argument 3: Consumer Group such as IngestionConsumer\n"+
					"Argument 4: Topic such as test\n"+
					"Argument 5: Output HDFS location such as hdfs:/user/19491/iotdata/\n"+
					"Argument 6: Batch Duration in Seconds\n");
			abc();
			System.exit(-1);
		}
		
		
		KAFKA_BROKER=getArgumentValue(args, "--broker");
		System.out.println("Broker "+KAFKA_BROKER);
		KAFKA_PORT=getArgumentValue(args, "--port");
		System.out.println("Broker port "+KAFKA_PORT);
		GROUPID=getArgumentValue(args, "--group");
		System.out.println("Group "+GROUPID);
		TOPIC=getArgumentValue(args, "--topic");
		System.out.println("Topic "+TOPIC);
		OUTPUT_LOCATION=getArgumentValue(args, "--output");
		System.out.println("Ouput Location "+OUTPUT_LOCATION);
		System.out.println("batch duration "+getArgumentValue(args, "--duration"));
		BATCH_DURATION=Integer.parseInt(getArgumentValue(args, "--duration"));
		MASTER="local[*]";
		APP_NAME="ZEAS STREAMING";
		
		SparkConf conf = new SparkConf().setMaster("yarn-cluster").setAppName(APP_NAME);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(BATCH_DURATION));
		Map<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put(TOPIC,1);
		JavaPairReceiverInputDStream<String, String> kafkaStream =
				KafkaUtils.createStream(jssc,
						KAFKA_BROKER+":"+KAFKA_PORT, GROUPID, topicMap);
		kafkaStream.print();
		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return StringUtils.join(Lists.newArrayList(COMMA.split(tuple2._2())), ",");
			}
		});
		lines.foreachRDD(new Function<JavaRDD<String>,  Void>() {
			public Void call(JavaRDD<String > rdd) throws Exception {
				Date today = new Date();
				String date = (new SimpleDateFormat("dd-MM-yyyy").format(today));
				System.out.println("Printing to file "+ OUTPUT_LOCATION + date+"-"+today.getTime()+"//n"+rdd.toString());
				//rdd.saveAsTextFile("hdfs:/user/19491/iotdata/" + (new SimpleDateFormat("dd-MM-yyyy").format(new Date())));
				rdd.saveAsTextFile(OUTPUT_LOCATION+date+"-"+today.getTime()+"/");
				return null;
			}
		});
		jssc.start();
		jssc.awaitTermination();

	}
	
	private static String getArgumentValue(String args[], String name) {
		for(int i=0;i<args.length;i++){
			if(args[i].equals(name)){
				if(args.length>=i+1){
					return args[i+1];
				}
				else{
					System.err.println("No value found for "+name);
				}
			}
		}
		System.err.println("No value found for "+name);
		return null;
	}
	static void abc(){
		
		
		
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("tesst");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(BATCH_DURATION));
		Map<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put("test",1);
		JavaPairReceiverInputDStream<String, String> kafkaStream =
				KafkaUtils.createStream(jssc,
						"10.6.185.142:6667", "GROUPID",topicMap );
		kafkaStream.print();
		
		jssc.start();
		jssc.awaitTermination();
	}
}
