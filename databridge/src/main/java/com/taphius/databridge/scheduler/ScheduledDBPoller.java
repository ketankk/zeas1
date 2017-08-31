package com.taphius.databridge.scheduler;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class ScheduledDBPoller extends QuartzJobBean{
	
	CustomTask task;

	private final static Logger LOG = Logger.getLogger(ScheduledDBPoller.class);
	
	public CustomTask getTask() {
		return task;
	}
	
	public void setTask(CustomTask tsk){
		task = tsk;
	}

	/**
	 * Its scheduled CRON job which polls Db at regular intervals.
	 */
	@Override
	protected void executeInternal(JobExecutionContext arg0)
			throws JobExecutionException {
		
		LOG.info("Inside ScheduledDBPoller to check new additions.");

		try {
			task.getDBUpdates();
		} catch (Exception e) {
			LOG.error("Exception while getting DBUpdates "+e.getMessage());
			e.printStackTrace();
		}

	}
}
