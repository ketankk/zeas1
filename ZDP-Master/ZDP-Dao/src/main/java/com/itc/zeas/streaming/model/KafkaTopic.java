package com.itc.zeas.streaming.model;

import lombok.Data;

/**
 * @author 20597
 * 
 * This class contains details of kafka topic
 * No. of partition in kafka, replication factor
 */
@Data
public class KafkaTopic {

	private String topicName;
	private int partitionCount;
	private int replicationFactor;

}
