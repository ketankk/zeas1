package com.itc.zeas.streaming.daoimpl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.utility.utils.CommonUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.streaming.model.StreamingEntity;

public class StreamingEntityManager {

	private static final Logger LOG = Logger.getLogger(StreamingEntityManager.class);

	public static void main(String[] args) throws Exception {
		new StreamingEntityManager().getStreamingEntity(null);
	}

	public List<StreamingEntity> getStreamingEntity(HttpServletRequest httpRequest) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		List<StreamingEntity> entities = new ArrayList<StreamingEntity>();

		CommonUtils commonUtils = new CommonUtils();

		String userId = commonUtils.extractUserNameFromRequest(httpRequest);
		String userNameList = "'" + userId + "'";

		ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
		Boolean isSuperUser = zdpUserAccess.isSuperUser(userId);
		String sQuery = ConnectionUtility.getSQlProperty("LIST_STREAMING_ENTITY");

		if (!isSuperUser) {
			Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
			// filter map for execute permission
			Iterator<Entry<String, Integer>> iterator = userNamePermissionMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, Integer> entry = iterator.next();
				Integer permission = entry.getValue();
				if (!(permission == UserManagementConstant.READ_EXECUTE
						|| permission == UserManagementConstant.READ_WRITE_EXECUTE)) {
					iterator.remove();
				}
			}
			for (String username : userNamePermissionMap.keySet()) {
				userNameList = userNameList + ",'" + username + "'";
			}

		}

		try {
			connection = ConnectionUtility.getConnection();

			if (isSuperUser) {
				sQuery = "select * from streaming_entity where is_active=1";
				preparedStatement = connection.prepareStatement(sQuery);

				LOG.debug("user is super user");
			} else {
				// if not super user,get entity from same group user
				sQuery = "select * from streaming_entity where created_by in(" + userNameList + ") and is_active=1";
				preparedStatement = connection.prepareStatement(sQuery);

				/*
				 * preparedStatement = connection.prepareStatement(sQuery);
				 * preparedStatement.setString(1, userNameList);
				 */
			}

			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				StreamingEntity entity = new StreamingEntity();
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				String type = rs.getString("type");
				entity.setType(type);
				String jsonData = rs.getString("json_data");

				ObjectMapper mapper = new ObjectMapper();
				JsonNode jsonNode = null;
				try {
					jsonNode = mapper.readValue(jsonData, JsonNode.class);
					entity.setJsonBlob(jsonNode);//

				} catch (JsonParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				entity.setSchemaJson(rs.getString("schema_json"));

				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setUpdatedBy(rs.getString("modified_by"));
				entity.setCreatedDate(rs.getTimestamp("created_on"));
				entity.setUpdatedDate(rs.getTimestamp("modified_on"));

				/*
				 * try {
				 * entity.setCreatedDate(rs.getTimestamp("last_modified")); }
				 * catch (SQLException e) { //e.printStackTrace(); }
				 */
				entities.add(entity);
				// System.out.println("Entity: " + entity.toString());

			}
		} catch (SQLException e) {
			LOG.error("SQLException while executing sql query string " + sQuery);
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		
		ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
	}return entities;

	}

	public StreamingEntity getStreamingEntityByName(String name) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String sQuery = ConnectionUtility.getSQlProperty("GET_STREAMING_ENTITY_BY_NAME");
		StreamingEntity entity = new StreamingEntity();

		try {
			connection = ConnectionUtility.getConnection();

			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, name);
			rs = preparedStatement.executeQuery();

			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));//
				System.out.println("json_data: " + rs.getString("json_data"));

				//
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setUpdatedBy(rs.getString("modified_by"));
				entity.setCreatedDate(rs.getTimestamp("created_on"));

			}
		} catch (SQLException e) {
			LOG.error("SQLException while executing sql query string " + sQuery);
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return entity;
	}
}
