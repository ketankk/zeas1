package com.zdp.kafka.prducer;


import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class FlightDelayProducer {
	
	static List<String> lines=null;
	static{
		Scanner sc=null;
		try {
			sc = new Scanner(new File("D:\\Work\\Iot\\flightdelay-sample1.csv"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		lines = new ArrayList<String>();
		while (sc.hasNextLine()) {
		  lines.add(sc.nextLine());
		}
	}
	public static void main(String[] args)  {
        Properties props = new Properties();
        props.put("metadata.broker.list", "ec2-54-174-149-226.compute-1.amazonaws.com:6667");
        //props.put("bootstrap.servers", "ec2-54-174-149-226.compute-1.amazonaws.com:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        //props.put("value.serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type","async");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        
		for (int i=0;	;i++){
        	producer.send(generateMessage("flightdelay"));
        	try {
        	    //Thread.sleep((int)Math.random()*10000);     
        		Thread.sleep(1000);
        	} catch(InterruptedException ex) {
        	   // Thread.currentThread().interrupt();
        	}
        }
}

	private static KeyedMessage<String, String> generateMessage(String topic) {
		int numRecords=lines.size();
		int index=(int)(Math.random()*numRecords);
		index=index>=numRecords?(numRecords-1):index;
		index=index<=0?1:index;
		System.out.println("Sending "+ topic+":::"+ lines.get(index));
		return new KeyedMessage<String, String>(topic, lines.get(index));	
	}
}


