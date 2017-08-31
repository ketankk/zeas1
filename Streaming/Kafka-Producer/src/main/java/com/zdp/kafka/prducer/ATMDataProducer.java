package com.zdp.kafka.prducer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

public class ATMDataProducer {
	public static void main(String[] args)  {
		String topic="zeastest1";

        Properties props = new Properties();
        props.put("metadata.broker.list", "10.6.185.142:6667");
        props.put("bootstrap.servers", "10.6.185.142:6667");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("linger.ms", 1);
        props.put("request.timeout.ms", 60000);

        
        props.put("producer.type","sync");
       // props.put("request.required.acks","all");
        Consumer(props);
        
       
}
	
	private static void Producer(Properties props){
		String topic="zeastest1";

		 Producer<String, String> producer = new KafkaProducer<String, String>(props);
	       
	        while(true){
	        	try {
					producer.send(generateMessage(topic)).get();
					System.out.println("sent..");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        	
	        	
	        	try {
	        		Thread.sleep(1000);
	        	} catch(InterruptedException ex) {
	        	   // Thread.currentThread().interrupt();
	        	}
	        }
	}

	private static void Consumer(Properties props){
		String topic="zeastest1";

		 KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(props);
	        consumer.subscribe(Arrays.asList(topic));
			Map<String, List<PartitionInfo>> topics = consumer.listTopics();
			System.out.println(consumer.subscription());
	        System.out.println(topics.toString());

	          ConsumerRecords<String, String> records = consumer.poll(100);
	        for (ConsumerRecord<String, String> record : records)
	            System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	   	
	}
	
	private static ProducerRecord<String, String> generateMessage(String topic) {
		String atmId= "ATM"+String.format("%04d",(int)(Math.random()*10000));
		String custId = "Cust"+String.format("%04d",(int)(Math.random()*10000));
		double typeRandom= Math.random();
		String tranType = (typeRandom<0.6)?"Cash Withdrawal" : (typeRandom<0.8)? "Cash Deposit" : "Cheque Deposit";
		int tranAmount = ((int)(Math.random()*100))*100;
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		String timeStamp = sdf.format(date);
		String status = (typeRandom<0.9)?"Success" : "Failed";
		System.out.println("Sending "+ topic+":::"+ atmId+","+custId+","+tranType+","+tranAmount+","+timeStamp+","+status);
		return new ProducerRecord<String, String>(topic, "kk",atmId+","+custId+","+tranType+","+tranAmount+","+timeStamp+","+status);	
	}
}
