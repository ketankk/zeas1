package com.itc.zeas.kafka.producer.constant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * contain twitter and Kafka configuration properties
 * 
 * @author 19217
 * 
 */
public class Constants {
	private static final Logger LOGGER = Logger.getLogger(Constants.class);
	/* Twitter oauth details */
	private static String twitterConsumerKey;
	private static String twitterConsumerSecret;
	private static String twitterAccessToken;
	private static String twitterAccessTokenSecret;

	private static String twitterTopic;
	private static String KafkaTopic;

	/* kafka producer config details */
	private static String metadataBroakerList;
	private static String serializerCalss;
	private static String producerType;
	private static String ackRequired;

	private static String httpProxyHost;
	private static int httpProxyPort;

	static {
		try {
			init();
			LOGGER.debug("twitterConsumerKey: " + twitterConsumerKey
					+ "twitterConsumerSecret: " + twitterConsumerSecret
					+ "twitterAccessToken: " + twitterAccessToken
					+ "twitterAccessTokenSecret: " + twitterAccessTokenSecret
					+ "twitterTopic: " + twitterTopic + "KafkaTopic: "
					+ KafkaTopic + "metadataBroakerList: "
					+ metadataBroakerList + "serializerCalss: "
					+ serializerCalss + "producerType: " + producerType
					+ "ackRequired: " + ackRequired + "httpProxyHost: "
					+ httpProxyHost + "httpProxyPort: " + httpProxyPort);
		} catch (IOException e) {
			LOGGER.error("PROBLEM WHILE INITIALIZING PROPERTY FROM KAFKA TWITTER PROPERTY FILE");
			e.printStackTrace();
		}
	}

	/*
	 * Twitter oauth details private String twitterConsumerKey =
	 * "UKcLjfDczeWQ65ZF60X95TSxy"; private String twitterConsumerSecret =
	 * "sK94arVjqmgsJUY13Jz3AlNdluiVLEfD1uQjWrizxLCVpuKptU"; private String
	 * twitterAccessToken =
	 * "3359356825-KPTi1HhFCVuy8omW8bWgHWSUNC6oIuOPZOrFfiI"; private String
	 * twitterAccessTokenSecret =
	 * "zWAMJCsCzYD0GPr8xcwRyVmHIouBF3yF87YSKvOJcs1de";
	 * 
	 * private String twitterTopic[] = { "bigdata" }; private String KafkaTopic
	 * = "bigdata";
	 * 
	 * kafka producer config details private String metadataBroakerList =
	 * "ec2-54-174-149-226.compute-1.amazonaws.com:6667"; private String
	 * serializerCalss = "kafka.serializer.StringEncoder"; private String
	 * producerType = "async"; private String ackRequired = "1";
	 * 
	 * private String httpProxyHost = "10.6.13.11"; private int httpProxyPort =
	 * 8080;
	 */
	/**
	 * initializes config fields by reading config property file
	 * 
	 * @throws IOException
	 */
	private static void init() throws IOException {
		InputStream inputStream = Constants.class
				.getResourceAsStream("/kafka-twitter.properties");
		Properties properties = new Properties();
		properties.load(inputStream);
		System.out.println(properties.toString());
		twitterConsumerKey = properties.getProperty("twitterConsumerKey");
		twitterConsumerSecret = properties.getProperty("twitterConsumerSecret");
		twitterAccessToken = properties.getProperty("twitterAccessToken");
		twitterAccessTokenSecret = properties
				.getProperty("twitterAccessTokenSecret");
		twitterTopic = properties.getProperty("twitterTopic");
		KafkaTopic = properties.getProperty("KafkaTopic");
		metadataBroakerList = properties.getProperty("metadataBroakerList");
		serializerCalss = properties.getProperty("serializerCalss");
		producerType = properties.getProperty("producerType");
		ackRequired = properties.getProperty("ackRequired");
		httpProxyHost = properties.getProperty("httpProxyHost");
		httpProxyPort = Integer.parseInt(properties
				.getProperty("httpProxyPort"));
	}

	public static String getTwitterConsumerKey() {
		return twitterConsumerKey;
	}

	public static String getTwitterConsumerSecret() {
		return twitterConsumerSecret;
	}

	public static String getTwitterAccessToken() {
		return twitterAccessToken;
	}

	public static String getTwitterAccessTokenSecret() {
		return twitterAccessTokenSecret;
	}

	public static String getTwitterTopic() {
		return twitterTopic;
	}

	public static String getKafkaTopic() {
		return KafkaTopic;
	}

	public static String getMetadataBroakerList() {
		return metadataBroakerList;
	}

	public static String getSerializerCalss() {
		return serializerCalss;
	}

	public static String getProducerType() {
		return producerType;
	}

	public static String getAckRequired() {
		return ackRequired;
	}

	public static String getHttpProxyHost() {
		return httpProxyHost;
	}

	public static int getHttpProxyPort() {
		return httpProxyPort;
	}

}
