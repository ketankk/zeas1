package com.itc.zeas.streaming.model;

import lombok.Data;

import java.util.List;

@Data
public class KafkaDetails {

    private List<String> kafkabrokerlist;
    private String zkhostName;
    private int zkhostPort;
    private List<KafkaTopic> topics;
    private String outputLocation;
    private int kafkaBrokerCount;


}

