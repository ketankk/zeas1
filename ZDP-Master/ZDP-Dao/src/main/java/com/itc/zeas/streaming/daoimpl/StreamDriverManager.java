package com.itc.zeas.streaming.daoimpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.streaming.model.StreamingEntity;
import com.itc.zeas.utility.connection.ConnectionUtility;

/**
 * 
 * @author 20597 Mar 8, 2017
 */

@Service(value = "streamDriverManager")
public class StreamDriverManager {

	// private Connection connection;
	private Logger LOG = Logger.getLogger(StreamDriverManager.class);
	Properties prop = new Properties();

	/**
	 * this method is to add entity to streaming_entity table of database
	 * 
	 * @author 20597
	 * @param entity
	 * @throws Exception
	 * @throws ZeasException
	 */
	public void addEntity(StreamingEntity entity) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			// check duplicate name

			StreamingEntity streamingEntity = getEntityByName(entity.getName());
			if (entity.getName().equalsIgnoreCase(streamingEntity.getName())) {
				throw new ZeasSQLException(ZeasErrorCode.DUPLICATE_ENTITY,
						"Entity with " + entity.getName() + " already exists", "");
			}
			String sQuery = ConnectionUtility.getSQlProperty("INSERT_STREAMING_ENTITY");
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, entity.getName());
			preparedStatement.setString(2, entity.getType());
			preparedStatement.setString(3, entity.getJsonblob());
			preparedStatement.setString(4, entity.getSchemaJson());

			// Currently sending '1' as isActive value
			preparedStatement.setBoolean(5, true);
			preparedStatement.setString(6, entity.getCreatedBy());
			preparedStatement.setString(7, entity.getUpdatedBy());
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}

	}

	public void updateEntity(StreamingEntity entity) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			// check duplicate name

			StreamingEntity streamingEntity = getEntityByName(entity.getName());
			// if some entity has got same name
			if ((entity.getName().equalsIgnoreCase(streamingEntity.getName()))
					&& (entity.getId() != streamingEntity.getId())) {
				throw new ZeasSQLException(ZeasErrorCode.DUPLICATE_ENTITY,
						"Entity with " + entity.getName() + " already exists", "");
			}
			String sQuery = ConnectionUtility.getSQlProperty("UPDATE_STREAMING_ENTITY");
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, entity.getName());
			preparedStatement.setString(2, entity.getJsonblob());
			preparedStatement.setString(3, entity.getUpdatedBy());
			preparedStatement.setString(4, String.valueOf(entity.getId()));

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
		}

	}

	/**
	 * this method is to get streaming entity from database
	 * 
	 * @throws Exception
	 */

	public StreamingEntity getEntityByName(String name) throws Exception {
		StreamingEntity entity = new StreamingEntity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = ConnectionUtility.getSQlProperty("GET_STREAMING_ENTITY_BY_NAME");
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, name);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setSchemaJson(rs.getString("schema_json"));
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created_on"));
				entity.setUpdatedBy(rs.getString("modified_by"));
				entity.setUpdatedDate(rs.getTimestamp("modified_on"));
				LOG.debug("name:" + name);
				// logger.debug("entity date:" +
				// entity.getUpdatedTimestamp().getTime());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}

		return entity;
	}

	public List<StreamingEntity> getStreamingEntity(HttpServletRequest httpServletRequest) throws Exception {
		StreamingEntityManager emgr = new StreamingEntityManager();
		List<StreamingEntity> entities = null;
		try {
			entities = emgr.getStreamingEntity(httpServletRequest);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return entities;
	}

	public void addStreamDriver(String appId, String consumer, String user) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = ConnectionUtility.getSQlProperty("INSERT_STREAM_DRIVER");
			String status = "RUNNING";
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);

			preparedStatement.setString(1, appId);
			preparedStatement.setString(2, consumer);
			preparedStatement.setString(3, status);
			preparedStatement.setString(4, user);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	public void updateStreamDriver(String driverId, String user) throws Exception {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {

			String sQuery = ConnectionUtility.getSQlProperty("UPDATE_STREAM_DRIVER");
			String status = "STOPPED";

			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);

			preparedStatement.setString(1, status);
			preparedStatement.setString(2, user);
			preparedStatement.setString(3, driverId);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}

		}

	}

	public Boolean deleteStreamingEntity(String entityName) throws ZeasException {
		Boolean status = false;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = ConnectionUtility.getConnection();

			String sql = "UPDATE streaming_entity SET is_active='0' WHERE name=?";
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, entityName);
			int rs = preparedStatement.executeUpdate();
			if (rs == 0)
				throw new ZeasException(ZeasErrorCode.ENTITY_DOESNOT_EXIST,
						"Entity with name " + entityName + " doesn't exist");
			status = true;

		} catch (SQLException e) {
			LOG.error("Exception due to " + e.getMessage());
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatement, connection);

		}
		return status;
	}

	/**
	 * get Streaming entity by id
	 * 
	 * @param id
	 * @return @StreamingEntity
	 * @throws Exception
	 */
	public StreamingEntity getEntityById(String id) throws Exception {
		StreamingEntity entity = new StreamingEntity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = ConnectionUtility.getSQlProperty("GET_STREAMING_ENTITY_BY_ID");
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, id);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setSchemaJson(rs.getString("schema_json"));
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created_on"));
				entity.setUpdatedBy(rs.getString("modified_by"));
				entity.setUpdatedDate(rs.getTimestamp("modified_on"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return entity;
	}

	/**
	 * method to get rule details, get jar location,class name,method name in
	 * string in same order
	 * 
	 * @param ruleName
	 * @return
	 * @throws SQLException
	 */
	public List<String> getRuleDetailsByName(String ruleName) throws SQLException {

		Connection conn = ConnectionUtility.getConnection();

		String sql = "Select * from zeas.transformation_rules where name=?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, ruleName);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String jarLoc = rs.getString("jar_location");
			String fqcn = rs.getString("fqcn");
			String mthdname = rs.getString("methodname");
			List<String> list = new ArrayList<String>();
			list.add(jarLoc);
			list.add(fqcn);
			list.add(mthdname);
			return list;
		}

		return null;
	}

}
