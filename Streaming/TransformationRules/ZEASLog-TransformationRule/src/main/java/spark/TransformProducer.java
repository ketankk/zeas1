
package spark;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class TransformProducer extends Thread {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;
    

	private  String messageStr;

    public static void main(String[] args){
      new TransformProducer("zeas-logs1",true,"hey").run();
    }
	public TransformProducer(String topic, Boolean isAsync,String message) {

		
    	
    	
    	
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.6.185.142:6667");
        props.put("bootstrap.servers", "10.6.185.142:6667");
       
        props.put("acks", "all");
		props.put("retries", "0");
		props.put("batch.size", "16384");
		props.put("auto.commit.interval.ms", "1000");
		props.put("linger.ms", "0");
		
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("block.on.buffer.full", "true");

        producer = new KafkaProducer(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.messageStr =message;
    }
	public void run() {
    	System.out.println("run");
       // int messageNo = 1;
    	System.out.println("MESSAGE********************");
    	System.out.println(messageStr);
    	abc(producer,messageStr,topic);

    }

static void  abc(KafkaProducer<String, String> producer,String msg,String topic){
	try {
			// send lots of messages
        System.out.println("Sending messages "+msg +" to topic "+topic);
        RecordMetadata rc;
        while(true){
			rc=producer.send(new ProducerRecord<String, String>(topic,
					msg)).get();

			//System.out.println(rc.offset());
			}

	} catch (Exception e) {
		e.printStackTrace();
	} 
}}
