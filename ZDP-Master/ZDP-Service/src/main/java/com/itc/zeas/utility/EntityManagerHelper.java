package com.itc.zeas.utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;

/**
 * created to manage module and entity table related complications.
 * 
 * @author 19217
 * 
 */
public class EntityManagerHelper {
	private static final Logger LOGGER = Logger
			.getLogger(EntityManagerHelper.class);

	

	public Boolean validateUserPermission(String userName, String entityType,
			String entityName, int permissionLevel) throws Exception {
		Boolean haveValidPermission = false;
		Long resourceId = null;
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
	
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			connection = ConnectionUtility.getConnection();
			// get Entity id
			String sQuery = ConnectionUtility.getSQlProperty("SELECT_ENTITY_BY_NAME");
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, entityName);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				
				resourceId = rs.getLong("id");
				LOGGER.info("ID is"+resourceId);
			}
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement,
					connection);
		}
		// check in dataset_permission table for given id
		if (resourceId != null) {
			haveValidPermission = zdpUserAccessImpl
					.validateUserPermissionForResource(
							UserManagementConstant.ResourceType.DATASET,
							userName, resourceId, permissionLevel);
		}
		return haveValidPermission;
	}

	
}
