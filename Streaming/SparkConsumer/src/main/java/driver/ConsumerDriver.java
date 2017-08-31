package driver;

import java.util.Arrays;

import org.apache.log4j.Logger;

import flink.FlinkConsumer;
import spark.SparkConsumer;

public class ConsumerDriver {
	public static Logger LOG = Logger.getLogger(ConsumerDriver.class);

	private static final String FLINK = "flinkC";
	private static final String SPARK = "sparkC";

	public static void main(String[] args) throws Exception {
		if (args.length < 13) {
			LOG.info("Atleast 12 Arguments required, provided only " + args.length + " Arguments \n"
					+ Arrays.toString(args));
			System.exit(-1);
		}
		String consumerType = args[0];

		LOG.info("Driver Class for Consumer type: " + consumerType);

		System.out.println(consumerType + " ConsumerType");
		// SparkConsumer.main(args);
		// if(true)return;
		switch (consumerType) {

		case FLINK:
			LOG.debug("Consumer Type is Flink");
			System.out.println("Consumer Type is Flink");
			FlinkConsumer.main(args);
			break;
		case SPARK:
			LOG.debug("Consumer Type is Spark");
			System.out.println("Consumer Type is Spark");
			SparkConsumer.main(args);
			break;
		default:
			LOG.debug("Consumer Type not Available");
		}
	}
}
