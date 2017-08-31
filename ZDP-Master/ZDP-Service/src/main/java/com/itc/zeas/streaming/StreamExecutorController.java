package com.itc.zeas.streaming;

import com.itc.zeas.streaming.model.StreamingEntity;

public class StreamExecutorController {

	private static final String PRODUCER = "producer";
	private static final String CONSUMER = "consumer";
	private static final String TRANSFORMATION = "transformation";

	private StreamExecutor executor;

	public StreamExecutorController(String type) {
		switch (type.toLowerCase()) {
		case PRODUCER:
			executor = new StreamProducerExecutor();
			break;
		case CONSUMER:
			executor = new StreamConsumerExecutor();
			break;
		case TRANSFORMATION:
			executor=new StreamTransExecutor();
		}
	}

	public String startStream(StreamingEntity entity) throws Exception {

		return executor.startStream(entity);

	}

	public boolean stopStream(StreamingEntity entity) {

		executor.stopStream(entity);

		return true;
	}
}
