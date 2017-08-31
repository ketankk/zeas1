package com.itc.zeas.project;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.ingestion.model.OozieJob;
import com.itc.zeas.ingestion.model.OozieStageStatusInfo;
import com.itc.zeas.model.PipelineJob;

/**
 * this class contains functionality for providing oozie job information for a
 * given pipeline job
 * 
 * @author 19217
 * 
 */
public class PipelineJobInfoController {

	private final static Logger LOGGER = Logger
			.getLogger(PipelineJobInfoController.class);

	/**
	 * provide information about oozie job associated with pipeline job
	 * identified by given pipelineJobId
	 * 
	 *            id of pipeline job for which oozie job information needs to be
	 *            provided
	 * @return an instance of PipelineJob
	 * @throws SQLException
	 */
	public static PipelineJob getPipelieJobInfo(String pipelineJobId)
			throws SQLException {
		LOGGER.debug("pipelieJobId: " + pipelineJobId);
		String oozieJobId = getOozieJobId(pipelineJobId);
		LOGGER.debug("oozieJobId: " + oozieJobId);
		OozieJob oozieJob = getOozieJobInfo(oozieJobId);
		PipelineJob pipelineJob = new PipelineJob();
		pipelineJob.setJobId(pipelineJobId);
		pipelineJob.setOozieJob(oozieJob);
		LOGGER.debug("pipelineJob: " + pipelineJob.toString());
		return pipelineJob;
	}

	/**
	 * gives oozie job id for given pipeline job id
	 * 
	 * @param pipelineJobId
	 *            pipeline job id for which oozie job id needs to be provided
	 * @return oozie job id corresponding to given pipeline job id
	 * @throws SQLException
	 */
	public static String getOozieJobId(String pipelineJobId) {
		String oozieJobId = null;
		String selectQuery = "SELECT OOZIEJOBID FROM PipelineOozieJobMap WHERE  PIPELINEJOBID="
				+ "?";
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(selectQuery);
			preparedStatement.setString(1, pipelineJobId);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				oozieJobId = resultSet.getString("OOZIEJOBID");
			}
		} catch (SQLException e) {
			LOGGER.error("SQLException occured while executing sql select query for retreiving oozie job id");
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet,
					preparedStatement, connection);
		}
		return oozieJobId;
	}

	/**
	 * provide oojie job information such as create time, start time, end time
	 * and status for given oozieJobId
	 * 
	 * @param oozieJobId
	 *            id of oozie job whose details needs to be provided
	 * @return an instance of OozieJob
	 * @throws SQLException
	 */
	public static OozieJob getOozieJobInfo(String oozieJobId) {
		String selectQuery = "select id, start_time, end_time, status from WF_ACTIONS where wf_id="
				+ "?";
		OozieJob oozieJob = new OozieJob();
		List<OozieStageStatusInfo> oozieStageStatusInfoList = new ArrayList<>();
		oozieJob.setJobId(oozieJobId);
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = ConnectionUtility.getOozieDbConnection();
			preparedStatement = connection.prepareStatement(selectQuery);
			preparedStatement.setString(1, oozieJobId);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				OozieStageStatusInfo oozieStageStatusInfo = new OozieStageStatusInfo();
				String id = (resultSet.getString("id"));
				String stage = id.substring(id.indexOf('@') + 1);
				oozieStageStatusInfo.setStage(stage);
				oozieStageStatusInfo.setStartTime(resultSet
						.getString("start_time"));
				oozieStageStatusInfo
						.setEndTime(resultSet.getString("end_time"));
				oozieStageStatusInfo.setStatus(resultSet.getString("status"));
				oozieStageStatusInfoList.add(oozieStageStatusInfo);
			}
		} catch (SQLException e) {
			LOGGER.error("SQLException occured while executing sql query for retreiving oozie job information");
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet,
					preparedStatement, connection);
		}
		oozieJob.setOozieStageStatusInfoList(oozieStageStatusInfoList);
		return oozieJob;
	}
}
