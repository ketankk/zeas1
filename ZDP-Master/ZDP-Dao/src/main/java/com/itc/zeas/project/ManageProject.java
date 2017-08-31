package com.itc.zeas.project;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.exceptions.PermissionException;

public class ManageProject {
	private static final Logger LOGGER = Logger.getLogger(ManageProject.class);

	public Boolean deleteProject(String userName, Long projectId)
			throws Exception {
		Boolean isProjDeleted = false;
		Boolean canDelete = false;
		// check whether user is creator of the project to be deleted
		ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
		canDelete = zdpUserAccess.canDeleteProject(projectId, userName);
		if (canDelete) {
			// delete the project and associated module from database
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			try {
				connection = ConnectionUtility.getConnection();
				String delProjectQuery = ConnectionUtility
						.getSQlProperty("DELETE_PROJECT");
				preparedStatement = connection
						.prepareStatement(delProjectQuery);
				preparedStatement.setLong(1, projectId);
				preparedStatement.executeUpdate();
				isProjDeleted = true;
			} catch (SQLException e) {
				LOGGER.error("SQLException while executing sql query string DEL_PROJECT_AND_ASSOCIATED_MODULE");
				e.printStackTrace();
			} finally {

				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (connection != null) {
					connection.close();
				}

			}
		} else {
			throw new PermissionException.NotACreatorException(
					"user needs to be a creator for deleting a project");
		}
		return isProjDeleted;
	}

	
	/**
	 * Gives id of Latest Oozie Job Execution for a project
	 * 
	 * @param projectId
	 *            id of running project
	 * @param version
	 *            version of project
	 * @return
	 * @throws Exception 
	 */
	public String getLatestOozieExecIdForAProject(Long projectId,
			Integer version) throws Exception {

		String latestOozieExecId = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = ConnectionUtility.getConnection();
			String latestOozieExecIdQuery = ConnectionUtility
					.getSQlProperty("GET_LATEST_OOZIE_EXECUTION_ID_FOR_A_PROJECT");
			preparedStatement = connection
					.prepareStatement(latestOozieExecIdQuery);
			preparedStatement.setLong(1, projectId);
			preparedStatement.setInt(2, version);
			resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				latestOozieExecIdQuery = resultSet.getString(1);
			}
		} catch (SQLException e) {
			LOGGER.error("SQLException while executing sql query string GET_LATEST_OOZIE_EXECUTION_ID_FOR_A_PROJECT");
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
		return latestOozieExecId;
	}
}
