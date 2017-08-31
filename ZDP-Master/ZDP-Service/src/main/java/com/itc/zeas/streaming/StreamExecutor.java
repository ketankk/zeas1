package com.itc.zeas.streaming;

import com.itc.zeas.streaming.model.StreamingEntity;

public interface StreamExecutor {

	String startStream(StreamingEntity entity) throws Exception;
	boolean stopStream(StreamingEntity entity);
}
