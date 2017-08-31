package com.itc.zeas.project;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.project.model.ModuleRunStatus;
import com.itc.zeas.project.model.ProjectRunStatus;

public class ProjectRunStatusHandler {
	private static Logger LOGGER = Logger.getLogger(ProjectRunStatusHandler.class);

	/**
	 * Gives Project Run Status for given project ID
	 * 
	 * @param projectId
	 *            project Id
	 * @param version
	 *            project version
	 * @return ProjectRunStatus instance representing project run status
	 * @throws Exception
	 */
	public ProjectRunStatus getProjectRunStatus(Long projectId, Integer version) throws Exception {
		ProjectRunStatus projectRunStatus = new ProjectRunStatus();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		projectRunStatus.setProjectId(projectId);
		projectRunStatus.setVersion(version);
		try {
			connection = ConnectionUtility.getConnection();
			String sQuery = ConnectionUtility.getSQlProperty("PROJECT_RUN_HISTORY");
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setLong(1, projectId);
			preparedStatement.setInt(2, version);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				String oozieJobId = rs.getString("oozie_id");
				LOGGER.debug("INSIDE FUNCTION getProjectRunStatus oozieJobId: " + oozieJobId);
				if (oozieJobId.equals("unknown")) {
					projectRunStatus.setRunStatus("submitted to oozie");

				} else {
					Long runId = rs.getLong("id");
					String runStatus = rs.getString("status");
					Date startTime = rs.getTimestamp("start_time");
					Date endTime = rs.getTimestamp("end_time");
					List<ModuleRunStatus> moduleRunStatusList = getmoduleRunStatusForGivenProject(runId);
					projectRunStatus.setStartTime(startTime);
					projectRunStatus.setEndTime(endTime);
					projectRunStatus.setRunStatus(runStatus);
					projectRunStatus.setModuleRunStatusList(moduleRunStatusList);
				}
				LOGGER.debug("PROJECT RUN STATUS for project id: " + projectId + " and version: " + version + " is: "
						+ projectRunStatus.getRunStatus());
			}
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error("SQLException for query  PROJECT_RUN_HISTORY" + e);
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return projectRunStatus;
	}

	private List<ModuleRunStatus> getmoduleRunStatusForGivenProject(Long runId) throws Exception {
		List<ModuleRunStatus> moduleRunStatusList = new ArrayList<>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			connection = ConnectionUtility.getConnection();
			String sQuery = ConnectionUtility.getSQlProperty("MODULE_RUN_STATUS_LIST_FOR_A_POJECT");
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setLong(1, runId);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Long moduleId = rs.getLong("module_id");
				Integer moduleVersion = rs.getInt("version");
				String runStatus = rs.getString("status");
				Date startTime = rs.getTimestamp("start_time");
				Date endTime = rs.getTimestamp("end_time");
				String details = rs.getString("details");
				ModuleRunStatus moduleRunStatus = new ModuleRunStatus();
				moduleRunStatus.setModuleId(moduleId);
				moduleRunStatus.setModuleVersion(moduleVersion);
				moduleRunStatus.setRunStatus(runStatus);
				moduleRunStatus.setStartTime(startTime);
				moduleRunStatus.setEndTime(endTime);
				moduleRunStatus.setDetails(details);
				moduleRunStatusList.add(moduleRunStatus);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error("SQLException for query  MODULE_RUN_STATUS_LIST_FOR_A_POJECT" + e);
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);

		}
		return moduleRunStatusList;
	}
}
