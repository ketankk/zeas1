package com.itc.zeas.kafka.producer;

import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.itc.zeas.kafka.producer.constant.Constants;

import kafka.javaapi.producer.Producer;

import kafka.producer.ProducerConfig;

/**
 * Kafka producer publish twitter feeds to kafka broker
 * 
 * @author 19217
 * 
 */
public class TwitterStreamProducer {
	private Producer<String, String> producer = null;
	private TwitterStream twitterStream = null;

	public static void main(String[] args) {
		TwitterStreamProducer twitterStreamProducer = new TwitterStreamProducer();
		twitterStreamProducer.initProducer();
		twitterStreamProducer.initTwitterStream();
		twitterStreamProducer.attachStatusListener();
		twitterStreamProducer.addFilterQuery();
	}

	/**
	 * add listener to TwitterStream
	 */
	private void attachStatusListener() {
		TwitterStatusListener twitterStatusListener = new TwitterStatusListener(
				producer);
		twitterStream.addListener(twitterStatusListener);
	}

	/**
	 * added filter query to TwitterStream
	 */
	private void addFilterQuery() {
		FilterQuery fq = new FilterQuery();
		fq.track(Constants.getTwitterTopic());
		twitterStream.filter(fq);
	}

	/**
	 * initializes kafka producer
	 */
	private void initProducer() {
		Properties props = new Properties();
		props.put("metadata.broker.list", Constants.getMetadataBroakerList());
		props.put("serializer.class", Constants.getSerializerCalss());
		props.put("producer.type", Constants.getProducerType());
		props.put("request.required.acks", Constants.getAckRequired());
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	/**
	 * creates TwitterStream stream for given configuration
	 */
	private void initTwitterStream() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(Constants.getTwitterConsumerKey());
		cb.setOAuthConsumerSecret(Constants.getTwitterConsumerSecret());
		cb.setOAuthAccessToken(Constants.getTwitterAccessToken());
		cb.setOAuthAccessTokenSecret(Constants.getTwitterAccessTokenSecret());
		cb.setHttpProxyHost(Constants.getHttpProxyHost());
		cb.setHttpProxyPort(Constants.getHttpProxyPort());
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	}
}
