package com.taphius.validation.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class CustomHDJob extends Job {

	//private JobState state = JobState.DEFINE;
	private JobStatus status;

	CustomHDJob(JobStatus status, JobConf conf) throws IOException {
		// super(status, conf);
		this(conf);
		setJobID(status.getJobID());
		this.status = status;
		//state = JobState.RUNNING;
		// TODO Auto-generated constructor stub
	}

	CustomHDJob(JobConf conf) throws IOException {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	public CustomHDJob(Configuration conf, String jobName)
			throws IOException {
		super(conf, jobName);
		// TODO Auto-generated constructor stub
	}

	public CustomHDJob(Configuration conf) throws IOException {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	public CustomHDJob() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}

	private StringBuilder sb = new StringBuilder();

	public boolean monitorAndPrintJob() throws IOException,
			InterruptedException {
		String lastReport = null;
		Job.TaskStatusFilter filter;
		Configuration clientConf = getConfiguration();
		filter = Job.getTaskOutputFilter(clientConf);
		JobID jobId = getJobID();
		// LOG.info("Running job: " + jobId);
		sb.append("Running job: " + jobId + "\n");
		int eventCounter = 0;
		boolean profiling = getProfileEnabled();
		IntegerRanges mapRanges = getProfileTaskRange(true);
		IntegerRanges reduceRanges = getProfileTaskRange(false);
		int progMonitorPollIntervalMillis = Job
				.getProgressPollInterval(clientConf);
		/* make sure to report full progress after the job is done */
		boolean reportedAfterCompletion = false;
		boolean reportedUberMode = false;
		while (!isComplete() || !reportedAfterCompletion) {
			if (isComplete()) {
				reportedAfterCompletion = true;
			} else {
				Thread.sleep(progMonitorPollIntervalMillis);
			}
			 if (getJobState() == JobStatus.State.PREP) {
			 continue;
			}
			if (!reportedUberMode) {
				reportedUberMode = true;
				// LOG.info("Job " + jobId + " running in uber mode : " +
				// isUber());
				 sb.append("Job " + jobId + " running in uber mode : " +
				 isUber()+"\n");
			}
			String report = (" map "
					+ StringUtils.formatPercent(mapProgress(), 0) + " reduce " + StringUtils
					.formatPercent(reduceProgress(), 0));
			if (!report.equals(lastReport)) {
				// LOG.info(report);
				sb.append(report + "\n");
				lastReport = report;
			}

			TaskCompletionEvent[] events = getTaskCompletionEvents(
					eventCounter, 10);
			eventCounter += events.length;
			printTaskEvents(events, filter, profiling, mapRanges, reduceRanges);
		}
		boolean success = isSuccessful();
		if (success) {
			// LOG.info("Job " + jobId + " completed successfully");
			sb.append("Job " + jobId + " completed successfully" + "\n");
		} else {
			// LOG.info("Job " + jobId + " failed with state " +
			// status.getState() +
			// " due to: " + status.getFailureInfo());
			// this.getJobState().KILLED
			String strError=super.getStatus().getFailureInfo();;
			sb.append("Job " + jobId + " failed with Error\n "+strError);
			sb.append("\n***************end of error**********\n");
		}
		Counters counters = getCounters();
		if (counters != null) {
			// LOG.info(counters.toString());
			sb.append(counters.toString());
		}
		return success;
	}

	public static CustomHDJob getInstance(Configuration conf)
			throws IOException {
		// create with a null Cluster
		JobConf jobConf = new JobConf(conf);
		return new CustomHDJob(jobConf);
	}

	// returns the logs for  submitted job
	public String getLogs() {
		return sb.toString();
	}

	private void printTaskEvents(TaskCompletionEvent[] events,
			Job.TaskStatusFilter filter, boolean profiling,
			IntegerRanges mapRanges, IntegerRanges reduceRanges)
			throws IOException, InterruptedException {
		for (TaskCompletionEvent event : events) {
			switch (filter) {
			case NONE:
				break;
			case SUCCEEDED:
				if (event.getStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
					// LOG.info(event.toString());
					sb.append(event.toString() + "\n");
				}
				break;
			case FAILED:
				if (event.getStatus() == TaskCompletionEvent.Status.FAILED) {
					// LOG.info(event.toString());
					sb.append(event.toString() + "\n");
					// Displaying the task diagnostic information
					TaskAttemptID taskId = event.getTaskAttemptId();
					String[] taskDiagnostics = getTaskDiagnostics(taskId);
					if (taskDiagnostics != null) {
						for (String diagnostics : taskDiagnostics) {
							System.err.println(diagnostics);
						}
					}
				}
				break;
			case KILLED:
				if (event.getStatus() == TaskCompletionEvent.Status.KILLED) {
					// LOG.info(event.toString());
					sb.append(event.toString() + "\n");
				}
				break;
			case ALL:
				// LOG.info(event.toString());
				sb.append(event.toString() + "\n");
				break;
			}
		}
	}
}
