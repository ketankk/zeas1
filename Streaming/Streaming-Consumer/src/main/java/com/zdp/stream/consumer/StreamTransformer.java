package com.zdp.stream.consumer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class StreamTransformer{
	
	private static final Pattern COMMA = Pattern.compile(",");
	private static final String GROUPID = "PUCKTIMETRANSFORMER";
	
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("LocationCoordinates");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
        Map<String,Integer> topicMap = new HashMap<String,Integer>();
        topicMap.put("test",1);
        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        "ec2-54-174-149-226.compute-1.amazonaws.com:2181", GROUPID, topicMap);
        
        
        //Converting the stream into lines, returning only values from the <key, value> pairs
        JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
              return tuple2._2();
            }
          });
        
        
        //Mapping a puck id to time spent by extracting the second and third element of the record.
        //Reducing the stream by summing and obtaining cumulative time spent at each puck by all customers.
        JavaPairDStream<String,Integer> elements = lines.mapToPair(new PairFunction<String, String, Integer>() {
        	public Tuple2<String, Integer> call(String s) {
        		ArrayList<String> elementList= Lists.newArrayList(COMMA.split(s));
  	          	return new Tuple2<String, Integer>(elementList.get(1), Integer.valueOf(elementList.get(2)));
  	        }
          }).reduceByKey(new Function2<Integer, Integer, Integer>() {
  	        public Integer call(Integer i1, Integer i2) {
  	          return i1 + i2;
  	        }
  	      });
       
        elements.print();
        
        //Converting Tuple <puckid, total_time_spent> to record_time, puckid, total_time_spent format
        JavaDStream<String> output = elements.map(new Function<Tuple2<String, Integer>, String>() {
            public String call(Tuple2<String, Integer> tuple2) {
            	Date today = new Date();
         		String date = (new SimpleDateFormat("yyyy-MM-dd hh:mm:00").format(today));
            	return date+","+tuple2._1()+","+tuple2._2();
            }
          });
        
        //Writing each RDD to file
        output.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String > rdd) throws Exception {
				Date today = new Date();
				String date = (new SimpleDateFormat("dd-MM-yyyy").format(today));
				System.out.println("Printing to file "+"hdfs:/user/19491/iotdata/" + date+"-"+today.getTime()+"//n"+rdd.toString());
				//rdd.saveAsTextFile("hdfs:/user/19491/iotdata/" + (new SimpleDateFormat("dd-MM-yyyy").format(new Date())));
				rdd.saveAsTextFile("hdfs:/user/19491/puckdata/"+date+"-"+today.getTime()+"/");
				return null;
			}
        });
        

        jssc.start();
        jssc.awaitTermination();

    }

}
