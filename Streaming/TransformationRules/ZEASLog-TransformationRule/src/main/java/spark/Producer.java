package spark;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every
 * so often, it will send a message to "slow-messages". This shows how messages
 * can be sent to multiple topics. On the receiving end, we will see both kinds
 * of messages but will also see how the two topics aren't really synchronized.
 */
public class Producer {
	public static void main(String[] args) throws IOException {
		// set up the producer
		KafkaProducer<String, String> producer = null;
		try {
		//	InputStream props = Resources.getResource("producer.props").openStream();
			Properties properties = new Properties();

			properties.put("bootstrap.servers", "10.6.185.142:6667");
			properties.put("acks", "all");
			properties.put("retries", "0");
			properties.put("batch.size", "16384");
			properties.put("auto.commit.interval.ms", "1000");
			properties.put("linger.ms", "0");
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("block.on.buffer.full", "true");
			//properties.load(props);
			producer = new KafkaProducer<>(properties);
		} catch (Exception e) {

		}

		try {
			for (int i = 0; i < 1000000; i++) {
				// send lots of messages
				producer.send(new ProducerRecord<String, String>("fast-messages",
						String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i))).get();

				// every so often send to a different topic
				if (i % 1000 == 0) {
					producer.send(new ProducerRecord<String, String>("fast-messages",
							String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					producer.send(new ProducerRecord<String, String>("summary-markers",
							String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
					//producer.flush();
					System.out.println("Sent msg number " + i);
				}

			}
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.close();
		}

	}
}
