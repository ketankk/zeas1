package com.itc.zeas.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.streaming.model.KafkaDetails;
import com.itc.zeas.streaming.model.KafkaTopic;
import com.itc.zeas.utility.utility.ConfigurationReader;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Helper class for using kafka/zookeeper
 * 
 * @author 20597 Mar 9, 2017
 */
public class KafkaHelper {

	Logger LOG = Logger.getLogger(KafkaHelper.class);

	/**
	 * use this method when broker details are no known
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws IOException
	 */
	public KafkaDetails kafkaDetails() throws ZeasException {
		return kafkaDetails(null);

	}

	/**
	 * this will return all details regarding kafka if broker details is
	 * provided it will get details of that boker otherwise it will read from
	 * config file and get brroker details running on cluster
	 * 
	 * @param kafkaBroker
	 * @return
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws IOException
	 */
	public KafkaDetails kafkaDetails(KafkaDetails kafkaBroker) throws ZeasException {

		if (kafkaBroker == null || kafkaBroker.getZkhostName() == null || kafkaBroker.getZkhostPort() == 0)
			kafkaBroker = readkafkaConfigFile();

		List<KafkaTopic> topics = getKafkaTopics(kafkaBroker.getZkhostName(), kafkaBroker.getZkhostPort());
		kafkaBroker.setTopics(topics);
		try {
			kafkaBroker.setKafkabrokerlist(getBrokerList());
		} catch (IOException | KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return kafkaBroker;
	}

	/**
	 * this will get static kafka details from configuration file
	 * 
	 * @return
	 */
	private KafkaDetails readkafkaConfigFile() throws ZeasException {

		KafkaDetails details = new KafkaDetails();
		details.setZkhostName(ConfigurationReader.getProperty("ZOOKEEPER_HOST"));
		details.setZkhostPort(Integer.parseInt(ConfigurationReader.getProperty("ZOOKEEPER_PORT")));
		details.setOutputLocation(ConfigurationReader.getProperty("STREAM_OUTPUT_LOCATION"));
		details.setKafkabrokerlist(Arrays.asList(ConfigurationReader.getProperty("BROKER_LIST").split(",")));
		return details;
	}

	/**
	 * method to get list of kafka topics on any host
	 * 
	 * @param host
	 *            zookeeper
	 * @param port
	 *            zookeeper
	 * @return
	 */
	public List<KafkaTopic> getKafkaTopics(String host, int port) {
		String zkhost = host + ":" + port;
		ZooKeeper zk;
		List<KafkaTopic> topics = new ArrayList<>();

		try {
			zk = new ZooKeeper(zkhost, 10000, null);
			List<String> topicNames = zk.getChildren("/brokers/topics", false);
			// int brokerCount = zk.getChildren("/brokers/ids", false).size();

			for (String topic : topicNames) {
				KafkaTopic kafkaTopic = new KafkaTopic();
				kafkaTopic.setTopicName(topic);
				try {
					int partitionNum = zk.getChildren("/brokers/topics/" + topic + "/partitions", false).size();
					kafkaTopic.setPartitionCount(partitionNum);

				} catch (Exception e) {
					e.printStackTrace();
				}
				// kafkaTopic.setReplicationFactor(replFac);
				topics.add(kafkaTopic);
			}

		} catch (IOException | KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("Topics from : zkHost =" + zkhost + ":" + port + " are" + topics);
		// System.out.println(topics);
		return topics;
	}

	public static void main(String[] args) throws ZeasException, IOException, KeeperException, InterruptedException {

		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for (Logger logger : loggers) {
			logger.setLevel(Level.OFF);
		}
		new KafkaHelper().getBrokerList();
	}

	/**
	 * for given zookeeper, tis will get list of brokers running
	 * 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private List<String> getBrokerList() throws ZeasException, IOException, KeeperException, InterruptedException {
		KafkaDetails det = readkafkaConfigFile();
		String zkhost = det.getZkhostName() + ":" + det.getZkhostPort();
		ZkUtils zk = ZkUtils.apply(zkhost, 10 * 1000, 8 * 1000, true);
		List<Broker> brokers = JavaConversions.seqAsJavaList(zk.getAllBrokersInCluster());
		List<String> brokerList = new ArrayList<>();
		ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
		for (Broker broker : brokers) {
			String host = broker.getBrokerEndPoint(listenerName).host();
			int port = broker.getBrokerEndPoint(listenerName).port();
			brokerList.add(host + ":" + port);

		}
		// System.out.println(brokerList);
		LOG.info("List of brokers on " + zkhost + " are " + brokerList);
		return brokerList;
	}

	/**
	 * it will create a topic in kafka first it validates the name, it should
	 * only contain alphanumeric and ._-
	 * 
	 * @param topic
	 * @return
	 * @throws ZeasException
	 */
	public KafkaTopic createTopic(KafkaTopic topic) throws ZeasException {
		KafkaDetails details = readkafkaConfigFile();
		if (!validateTopic(topic.getTopicName())) {
			LOG.error("Topic name can't contain special character " + topic.getTopicName());
			throw new ZeasException(ZeasErrorCode.NOT_VALID_STRING, "Topic name can't contain special character ",
					"Topic name can't contain special character, only alphanumerics, '.', '_' and '-' ");
		}
		String zkConnect = details.getZkhostName() + ":" + details.getZkhostPort();
		ZkClient zkClient = new ZkClient(zkConnect, 10 * 1000, 8 * 1000, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		Properties prop = new Properties();

		AdminUtils.createTopic(zkUtils, topic.getTopicName(), topic.getPartitionCount(), topic.getReplicationFactor(),
				prop,RackAwareMode.Enforced$.MODULE$);
		LOG.info("Topic created, Topic name " + topic);
		zkClient.close();
		return topic;

	}

	/**
	 * validate if kafka topic name is valid it should only conatain
	 * alphanumeric and ./- no other special character
	 * 
	 * @param TopicName
	 * @return true if valid name
	 */

	static public boolean validateTopic(String TopicName) {

		String pattern = "[a-zA-Z0-9\\._\\-]*";
		return Pattern.compile(pattern).matcher(TopicName).matches();

	}
}
