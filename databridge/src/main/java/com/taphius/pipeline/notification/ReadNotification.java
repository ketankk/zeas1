package com.taphius.pipeline.notification;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Properties;

import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

import com.itc.zeas.v2.pipeline.PipelineExecutor;
import com.itc.zeas.v2.pipeline.ProjectUtil;
import com.itc.zeas.project.model.ProjectEntity;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;

public class ReadNotification {

	public Properties prop = new Properties();
	private OutputStream output;
	public static Logger LOG = Logger.getLogger(ReadNotification.class);
	
	/*public static void main(String args[]){
		ReadNotification rn = new ReadNotification();
		try {
			rn.checkNotification();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	public String checkNotification() throws SQLException {
		String pipeline = "";
		boolean modified = false;
		try {
			InputStream inputStream = new FileInputStream(
					ConfigurationReader.getProperty("NOTIFY_FILE_PATH")
							+ "/notify");
			//InputStream inputStream = new FileInputStream("D:\\notify");
			prop.load(inputStream);
			inputStream.close();
			for (String pipelineName : prop.stringPropertyNames()) {
				if ("START".equalsIgnoreCase(prop.getProperty(pipelineName))) {
					LOG.info("Found newly added pipeline. Going to start pipeline process named - "
							+ pipelineName);

					/**
					 * For every pipeline run request a new thread will be
					 * spawned.
					 */
					/**
					 * adding entry in Table 'project_history' for this
					 * pipelineName with oozieJobId as "unknown"
					 */
					Long projectRunId = makeEntryInProjectRunHistory("unknown",
							pipelineName);
					PipelineExecutor pipeExe = new PipelineExecutor(
							pipelineName, projectRunId);
					Thread pipeLineThread = new Thread(pipeExe, pipelineName);
					pipeLineThread.start();
					modified = true;

					LOG.info("Started execution of pipeline - " + pipelineName);
					prop.setProperty(pipelineName, "RUNNING");
				}
			}
			if (modified) {
				output = new FileOutputStream(
						ConfigurationReader.getProperty("NOTIFY_FILE_PATH")
								+ "/notify");
				// save Notify file with status updated.
				prop.store(output, null);
			}

		} catch (IOException ioE) {
			LOG.error("Error - " + ioE.getMessage());
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					LOG.error("Error saving Notify file -" + e.getMessage());
				}
			}
		}
		return pipeline;
	}

	/**
	 * adds entry in Table 'project_history' for the pipeline
	 * 
	 * @param oozieJobId
	 * @param projectName
	 * @return project run id
	 */
	private static Long makeEntryInProjectRunHistory(String oozieJobId,
			String projectName) {
		LOG.debug("making an entry in ProjectRunHistory table for project: "
				+ projectName);
		ZDPDataAccessObject dataAccessObject = new ZDPDataAccessObjectImpl();
		// create project run history
		ProjectEntity projectProjectEntity = ProjectUtil.getProjectHistoryEntity(projectName,
				oozieJobId);
		ProjectEntity projHistempProjectEntity = dataAccessObject.addEntity(projectProjectEntity);
		//TODo check long
		Long project_run_id = Long.valueOf(projHistempProjectEntity.getId());
		return project_run_id;
	}
}
