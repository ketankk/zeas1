package com.itc.zeas.streaming.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.itc.zeas.profile.model.Entity;
import lombok.Data;

/**
 * @author ketan
 *
 * This clas is a model class for holding Streaming related entity
 * it mainly interacts with streaming_entity table
 *
 */
//this Entity import should be from model
@Data
public class StreamingEntity extends Entity {

    public String startAt;
    public String stopAt;
    public String startBy;
    public String stopBy;
    private int count;
    private String schemaJson;
    private JsonNode jsonBlob;
    private KafkaDetails kafkaDetails;
    private String topic;

}
