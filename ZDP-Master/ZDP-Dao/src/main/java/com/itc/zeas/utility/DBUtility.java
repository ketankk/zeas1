package com.itc.zeas.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.connection.ConnectionUtility;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DBUtility {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DBUtility.class);

	private static BasicDataSource mysqlDataSrc = null;

	/**
	 * Gives database connection
	 *
	 * @return database connection
	 */
	public static Connection getConnection() {
		LOGGER.debug("Trying to get the connection");
		//Connection connection = ZeasDataSource.INSTANCE.getConnection();
		Connection connection =ConnectionUtility.getConnection();
		LOGGER.debug("Got the connection successfully");
		return connection;
	}

	public static BasicDataSource getMySQLDataSource() {
		Properties props = new Properties();
		InputStream fis = null;

		try {
			fis = new FileInputStream(System.getProperty("user.home")
					+ "/zeas/Config/config.properties");
			props.load(fis);
			mysqlDataSrc = new BasicDataSource();
			mysqlDataSrc.setUrl(props.getProperty("DB_URL"));
			mysqlDataSrc.setUsername(props.getProperty("USERNAME"));
			mysqlDataSrc.setPassword(props.getProperty("PASSWD"));
			mysqlDataSrc.setDriverClassName(props.getProperty("DRIVER"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return mysqlDataSrc;
	}

	public static String getSQlProperty(String param) throws IOException {/*
		InputStream inputStream = null;
		String sqlQuery = null;

		try {
			Properties prop = new Properties();
			inputStream = DBUtility.class.getClassLoader().getResourceAsStream(
					"SQLEditor.properties");
			prop.load(inputStream);
			sqlQuery = prop.getProperty(param);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
		return sqlQuery;

	*/
		InputStream inputStream = null;
		String sqlQuery = null;

		try {
			Properties prop = new Properties();
			// inputStream = ConnectionUtility.class.getClassLoader()
			// .getResourceAsStream("SQLEditor.properties");
			inputStream = new FileInputStream(System.getProperty("user.home")
					+ "/zeas/Config/SQLEditor.properties");
			prop.load(inputStream);
			sqlQuery = prop.getProperty(param);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return sqlQuery;


	}

	public static String getJSON_DATA(String entity_name) throws SQLException {
		String json_Data = "";
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = DBUtility.getSQlProperty("GET_JSON_FOR_ENTITY");
			LOGGER.info("Check query - " + sQuery);
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, entity_name);
			rs = preparedStatement.executeQuery();
			LOGGER.info("Check query - " + rs);
			while (rs.next()) {
				LOGGER.info("inside rs loop- " + sQuery);
				json_Data = rs.getString("JSON_DATA");
			}
		} catch (SQLException | IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			DBUtility.releaseConnectionResources(rs, preparedStatement,
					connection);
		}
		return json_Data;

	}

	public static Entity getEntityDetails(String entity_name)
			throws SQLException {
		Entity entity = new Entity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = DBUtility.getSQlProperty("ENTITY_DETAILS");
			LOGGER.info("Check query - " + sQuery);
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, entity_name);
			rs = preparedStatement.executeQuery();
			LOGGER.info("Check query - " + rs);
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setActive(rs.getBoolean("is_active"));
				/*
				 * entity.setCreatedBy(rs.getString("created_by"));
				 * entity.setCreatedDate(rs.getDate("created"));
				 * entity.setUpdatedBy(rs.getString("updated_by"));
				 * entity.setUpdatedDate(rs.getDate("last_modified"));
				 */
			}
		} catch (SQLException | IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			DBUtility.releaseConnectionResources(rs, preparedStatement,
					connection);
		}
		return entity;

	}

	public static Entity getProjectDetails(String projNameAndVersion) {

		Entity entity = new Entity();
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_PROJECT");
			LOGGER.info("Check query - " + sQuery);
			connection = DBUtility.getConnection();
			ps = connection.prepareStatement(sQuery);
			ps.setInt(1, Integer.parseInt(projNameAndVersion.split("-")[0]));
			ps.setInt(2, Integer.parseInt(projNameAndVersion.split("-")[1]));
			rs = ps.executeQuery();
			LOGGER.info("Check query - " + rs);
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setJsonblob(rs.getString("design"));
				entity.setVersion(rs.getString("version"));
			}
		} catch (SQLException | IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			DBUtility.releaseConnectionResources(rs, ps, connection);
		}

		return entity;

	}

	public static Entity getTransformationDetails(String transNameAndVersion) {

		Entity entity = new Entity();
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_MODULE");
			LOGGER.info("Check query - " + sQuery);
			connection = DBUtility.getConnection();
			ps = connection.prepareStatement(sQuery);
			ps.setInt(1, Integer.parseInt(transNameAndVersion.split("-")[0]));
			ps.setInt(2, Integer.parseInt(transNameAndVersion.split("-")[1]));
			rs = ps.executeQuery();
			LOGGER.info("Check query - " + rs);
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName("Transformation");
				entity.setJsonblob(rs.getString("properties"));
				entity.setVersion(rs.getString("version"));
				entity.setType(rs.getString("component_type"));
			}
		} catch (SQLException | IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			DBUtility.releaseConnectionResources(rs, ps, connection);
		}
		return entity;

	}

	public static List<String> getColumns(String schema) {
		List<String> columns = new ArrayList<String>();
		try {
			Entity dataschema = getEntityDetails(schema);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(dataschema.getJsonblob());
			JsonNode colAttrs = rootNode.path("dataAttribute");

			for (JsonNode jsonNode : colAttrs) {
				columns.add(jsonNode.path("Name").textValue());
			}
		} catch (IOException | SQLException ex) {
			ex.printStackTrace();
		}
		return columns;
	}

	/*
	 * gets columns and datatype list name and type is seperated by :(colon).
	 * eg. id:int, name:string
	 */
	public static List<String> getColumnAndDatatype(String schema) {
		List<String> columns = new ArrayList<String>();

		try {
			Entity dataschema = getEntityDetails(schema);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(dataschema.getJsonblob());
			JsonNode colAttrs = rootNode.path("dataAttribute");

			for (JsonNode jsonNode : colAttrs) {
				columns.add(jsonNode.path("Name").textValue() + ":"
						+ jsonNode.path("dataType").textValue());
			}
		} catch (IOException | SQLException ex) {
			ex.printStackTrace();
		}
		return columns;
	}

	/**
	 * This method is to fetch entity details for a given entityId
	 *
	 * @param id
	 * @return Entity
	 * @throws IOException
	 */
	public static Entity getEntityById(Integer id) {
		Entity entity = new Entity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_ENTITY_BY_ID");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			// preparedStatement.setString(2,type);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setActive(rs.getBoolean("is_active"));
				/*
				 * entity.setCreatedBy(rs.getString("created_by"));
				 * entity.setCreatedDate(rs.getDate("created"));
				 * entity.setUpdatedBy(rs.getString("updated_by"));
				 * entity.setUpdatedDate(rs.getDate("last_modified"));
				 */
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}

		return entity;
	}

	/**
	 * Utility method fetches Entity Id for given Entity name.
	 *
	 * @param entity_name
	 *            String name of Entity
	 * @return String Entity ID.
	 */
	public static String getEntityId(String entity_name) {
		int id = 1;
		try {
			id = (int) getEntityDetails(entity_name).getId();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return "" + id;
	}

	/**
	 * this method is used to add processed pipeline to database
	 *
	 * @throws SQLException
	 */
	public static void saveProcessedPipeline(String pipelineName,
											 String outputDataSet) throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_PIPELINE");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, pipelineName);
			preparedStatement.setString(2, outputDataSet);
			preparedStatement.executeUpdate();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	/**
	 * this method is used to add processed pipeline to database
	 *
	 * @throws SQLException
	 */
	public static void updateStageProgress(int pipelineId, String stageName,
										   String status, Timestamp updatedTime) throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility
					.getSQlProperty("INSERT_PIPELINE_PROGRESS");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, pipelineId);
			preparedStatement.setString(2, stageName);
			preparedStatement.setString(3, status);
			preparedStatement.setTimestamp(4, updatedTime);
			preparedStatement.executeUpdate();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	/**
	 * this method is used to add processed pipeline to database
	 *
	 * @throws SQLException
	 */
	public static void cleanUpPipelineProgress(int pipelineId)
			throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility
					.getSQlProperty("DELETE_PIPELINE_PROGRESS");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, pipelineId);
			preparedStatement.executeUpdate();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	public static void addJobMappingInDb(int pipelineId, String oozieJobId)
			throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("ADD_JOB_MAPPING");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, pipelineId);
			preparedStatement.setString(2, oozieJobId);
			preparedStatement.executeUpdate();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatement,
					connection);
		}

	}

	/**
	 * release the connection resource passed to this function
	 *
	 * @param resultSet
	 * @param preparedStatement
	 * @param connection
	 */
	public static void releaseConnectionResources(ResultSet resultSet,
												  PreparedStatement preparedStatement, Connection connection) {
		LOGGER.debug("Releasing the connection resources");
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * release the connection resource passed to this function
	 *
	 * @param preparedStatement
	 * @param connection
	 */
	public static void releaseConnectionResources(
			PreparedStatement preparedStatement, Connection connection) {
		LOGGER.debug("Releasing the connection resources");
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
