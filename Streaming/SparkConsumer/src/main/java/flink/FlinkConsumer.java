package flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Driver class to consume messages from kafka and write to hdfs after
 * transforming
 * 
 * @author 20597
 *
 */
public class FlinkConsumer {


	private static String KAFKA_BROKER;
	private static String KAFKA_PORT;
	private static String GROUPID;
	private static String TOPIC;
	private static String OUTPUT_LOCATION;
	private static int BATCH_DURATION;
	private static String MASTER;
	private static String APP_NAME;
	private static String USER;
	public static void main(String[] args) throws Exception {
		String path="hdfs://10.6.185.142:8020/user/zeas/stream/flink";
		transFormStream(consumeFromKafka(setBrokerProp()), path);
	}

	/**
	 * Method to consume from kafka broker
	 * 
	 * @param properties
	 * @throws Exception
	 */

	static DataStream<String> consumeFromKafka(Properties properties) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer082<>("fast-messages", new SimpleStringSchema(), properties));
		String path="hdfs://10.6.185.142:8020/user/zeas/stream/flink";

		stream.print().setParallelism(1);
		stream.writeAsText(path);

		env.execute("Socket Window WordCount");
		return stream;
	}

	static void transFormStream(DataStream<String> stream, final String path) {
		final DataStream<String> tranStream = stream.flatMap(new FlatMapFunction<String, String>() {
			public void flatMap(String value, Collector<String> out) throws Exception {
				for (String word : value.split("\\s")) {
					out.collect(word);
					//.writeAsText(path);

				}
			}
		});
		tranStream.writeAsText(path);

	}

	static void pushToHdfs(DataStream<String> stream) {

	}

	/**
	 * set properties of kafka broker for flume
	 * 
	 * @return
	 */

	private static Properties setBrokerProp() {

		// Read these from conf file
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "10.6.185.142:6667");
		properties.put("fetch.min.bytes", "50000");
		properties.put("max.partition.fetch.bytes", "2097152");
		properties.put("group.id", "test");
		properties.put("enable.auto.commit", "true");

		properties.put("receive.buffer.bytes", "262144");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("session.timeout.ms", "10000");
		properties.setProperty("zookeeper.connect", "10.6.185.142:2181");
		return properties;

	}
void aa(){
		String[] args = null;
	KAFKA_BROKER=getArgumentValue(args, "--broker");
	System.out.println("Broker "+KAFKA_BROKER);
	KAFKA_PORT=getArgumentValue(args, "--port");
	System.out.println("Broker port "+KAFKA_PORT);
	GROUPID=getArgumentValue(args, "--group");
	System.out.println("Group "+GROUPID);
	TOPIC=getArgumentValue(args, "--topic");
	System.out.println("Topic "+TOPIC);
	USER=getArgumentValue(args, "--user");
	
	OUTPUT_LOCATION="/user/zeas/stream/"+USER;
	System.out.println("Ouput Location "+OUTPUT_LOCATION);
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

}