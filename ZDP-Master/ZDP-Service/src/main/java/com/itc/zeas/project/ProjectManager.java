package com.itc.zeas.project;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.exceptions.PermissionException;
import com.itc.zeas.exceptions.SqlIoException;
import com.itc.zeas.project.model.ProjectRunStatus;

/**
 * 
 * @author 19217
 * 
 */
public class ProjectManager {
	private static final Logger LOGGER = Logger.getLogger(ProjectManager.class);

	public ResponseEntity<String> deleteProject(String userName, Long projectId) throws Exception {
		LOGGER.debug("deleting project with id: " + projectId
				+ "initiated by user " + userName);
		ResponseEntity<String> responseEntity;
		ManageProject manageProject = new ManageProject();
		try {
			Boolean isProjDeleted = manageProject.deleteProject(userName,
					projectId);
			if (isProjDeleted) {
				responseEntity = new ResponseEntity<String>(
						"Successfully deleted the project", HttpStatus.OK);
			} else {
				responseEntity = new ResponseEntity<String>(
						"Failed to delete the Project",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} catch (PermissionException.NotACreatorException notACreatorException) {
			LOGGER.info("user needs to be a creator for deleting a project");
			responseEntity = new ResponseEntity<String>("Failed "
					+ notACreatorException.getMessage(), HttpStatus.FORBIDDEN);
		}
		return responseEntity;
	}

	// public String killLatestExecutionOfProject(Long projectId, Integer
	// version) {
	// String oozieExecutionId = getLatestOozieExecIdForAProject(projectId,
	// version);
	// return null;
	// }
	/**
	 * Gives Project Run Status for given project ID
	 * 
	 * @param projectId
	 *            project ID
	 * @param version
	 *            Project Version
	 * @param httpRequest
	 *            HttpServletRequest instance
	 * @return ResponseEntity instance
	 * @throws Exception 
	 */
	public ResponseEntity<Object> getProjectRunStatus(Long projectId,
			Integer version, HttpServletRequest httpRequest) throws Exception {
		LOGGER.debug("inside function getProjectRunStatus");
		ResponseEntity<Object> responseEntity = null;
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		Boolean haveValidPermission = false;
		try {
			haveValidPermission = zdpUserAccessImpl
					.validateUserPermissionForResource(
							UserManagementConstant.ResourceType.PROJECT,
							userName, projectId, UserManagementConstant.READ);
		} catch (SqlIoException.IoException exception) {
			new ResponseEntity<Object>(
					HttpStatus.INTERNAL_SERVER_ERROR);
		}catch (SqlIoException.SqlException exception) {
			new ResponseEntity<Object>(
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		if (haveValidPermission) {
			ProjectRunStatusHandler projectRunStatusHandler = new ProjectRunStatusHandler();
			ProjectRunStatus projectRunStatus = projectRunStatusHandler
					.getProjectRunStatus(projectId, version);
			responseEntity = new ResponseEntity<Object>(projectRunStatus,
					HttpStatus.OK);
		} else {
			responseEntity = new ResponseEntity<Object>(
					"don't have enough permission to see project run status",
					HttpStatus.OK);
		}
		return responseEntity;
	}
}
