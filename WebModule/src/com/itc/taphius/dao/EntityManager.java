package com.itc.taphius.dao;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.persistence.PrePersist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.itc.taphius.controller.PipelineJobInfoController;
import com.itc.taphius.model.DataIngestionLog;
import com.itc.taphius.model.Entity;
import com.itc.taphius.model.MLAnalysis;
import com.itc.taphius.model.OozieJob;
import com.itc.taphius.model.OozieStageStatusInfo;
import com.itc.taphius.model.PipelineStageLog;
import com.itc.taphius.model.ProcessedPipeline;
import com.itc.taphius.model.Profile;
import com.itc.taphius.utility.ConfigurationReader;
import com.itc.taphius.utility.DBUtility;
import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasSQLException;

/**
 * @author 16765
 * 
 */

public class EntityManager {
	private static String delete;
	private Connection connection;
	private Logger logger = Logger.getLogger(EntityManager.class);
	Properties prop = new Properties();
	private String hdfsPath;

	public EntityManager() {
		// connection = DBUtility.getConnection();
	}

	/**
	 * this method is used to list all entities
	 * 
	 * @param type
	 * @return List of Entity
	 * @throws SQLException
	 */
	public List<Entity> getEntity(String type) {
		List<Entity> entities = new ArrayList<Entity>();

		try {
			String sQuery = DBUtility.getSQlProperty("LIST_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, type);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Entity entity = new Entity();
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));//
				System.out.println("type: " + type);
				System.out.println("json_data: " + rs.getString("json_data"));

				//
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created"));
				entity.setUpdatedBy(rs.getString("updated_by"));
				entity.setCreatedDate(rs.getTimestamp("last_modified"));
				entities.add(entity);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return entities;
	}

	/**
	 * This method is used to list all Profiles, every Profile is representation
	 * of consolidated information about data source, data set,schema and
	 * scheduler
	 * 
	 * @return List of Profile
	 */
	public List<Profile> getProfiles() {
		logger.debug("inside function getProfile");
		List<Profile> profileList = new ArrayList<Profile>();
		try {
			String sQuery = DBUtility.getSQlProperty("LIST_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, "dataIngestion");
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Profile profile = new Profile();
				int schedulerId = rs.getInt("id");
				profile.setScedulerID(schedulerId);
				String jobStatus = getJobStatus(schedulerId);
				profile.setJobStatus(jobStatus);

				String name = rs.getString("name");
				profile.setName(name);

				String jsonBlob = rs.getString("json_data");
				// converting jsonblob string to json object using jackson
				// libary
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(jsonBlob);
				String dataSourceLoc = (rootNode.get("dataSource"))
						.getTextValue();
				profile.setSourcePath(dataSourceLoc);

				String destinationDatasetName = (rootNode
						.get("destinationDataset")).getTextValue();

				profile.setSchedulerFrequency(rootNode.get("frequency")
						.getTextValue());
				logger.debug("dataSourceLoc: " + dataSourceLoc);
				Entity entityFromDataSource = getEntityByName(dataSourceLoc);
				String enityJsonBlob = entityFromDataSource.getJsonblob();
				logger.debug("entityFromDataSource json blob: "
						+ entityFromDataSource.getJsonblob());
				if (enityJsonBlob != null) {
					rootNode = mapper.readTree(enityJsonBlob);
					if (!rootNode.get("sourcerType").getTextValue()
							.equalsIgnoreCase("RDBMS")) {
						JsonNode locationNode = rootNode.get("location");
						if (locationNode != null) {
							String sourceLocation = locationNode.getTextValue();
							profile.setSourcePath(sourceLocation);
						}
					}
					profile.setType(entityFromDataSource.getFormat());
					profile.setDatasourceid(entityFromDataSource.getId());
					String schemaName = null;
					try {
						schemaName = (rootNode.get("schema")).getTextValue();
						String sourceFormat = (rootNode.get("format"))
								.getTextValue();
						profile.setSourceFormat(sourceFormat);
					} catch (Exception e) {
						logger.debug("exception while retrieving schema: "
								+ enityJsonBlob);
					}
					//
					delete = schemaName;
					if (schemaName != null) {

						Entity entityForGivenSchemaName = getEntityByName(schemaName);
						profile.setUser(entityForGivenSchemaName.getCreatedBy());
						profile.setSchemaId(entityForGivenSchemaName.getId());
						profile.setSchemaName(entityForGivenSchemaName
								.getName());
						String schemaEntityJsonBlob = entityForGivenSchemaName
								.getJsonblob();
						profile.setSchemaJsonBlob(schemaEntityJsonBlob);
						Date modificationDate = entityForGivenSchemaName
								.getUpdatedDate();
						profile.setSchemaModificationDate(modificationDate);
					}
				}

				logger.debug("datasetDestinationLoc: " + destinationDatasetName);
				Entity entityFromDataDestination = getEntityByName(destinationDatasetName);
				if (entityFromDataDestination.getJsonblob() != null) {
					rootNode = mapper.readTree(entityFromDataDestination
							.getJsonblob());
					String destinationLocation = (rootNode.get("location"))
							.getTextValue();
					profile.setDatasetID(entityFromDataDestination.getId());
					profile.setDataSetTargetPath(destinationLocation);
				}
				profile.setDataSetName(destinationDatasetName);
				profileList.add(profile);
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }
		Collections.sort(profileList);
		return profileList;
	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws ZeasSQLException
	 * @throws SQLException
	 */
	public void addEntity(Entity entity) throws ZeasSQLException {
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, entity.getName());
			preparedStatement.setString(2, entity.getType());
			preparedStatement.setString(3, entity.getJsonblob());
			// Currently sending '1' as isActive value
			preparedStatement.setBoolean(4, true);
			preparedStatement.setString(5, entity.getCreatedBy());
			preparedStatement.setString(6, entity.getUpdatedBy());
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			logger.info("EntityManager.addEntity(): SQLException: "
					+ e.getMessage());
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws SQLException
	 */
	public Entity getEntityByName(String name) {
		Entity entity = new Entity();
		try {
			String sQuery = DBUtility.getSQlProperty("GET_ENTITY_BY_NAME");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, name);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created"));
				entity.setUpdatedBy(rs.getString("updated_by"));
				entity.setUpdatedDate(rs.getTimestamp("last_modified"));
				logger.debug("name:" + name);
				// logger.debug("entity date:" +
				// entity.getUpdatedTimestamp().getTime());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return entity;
	}

	/**
	 * this method is to update entity details
	 * 
	 * @param Entity
	 *            ds
	 * @param type
	 * @param entityId
	 * @throws SQLException
	 */
	public void updateEntity(Entity ds, String type, Integer entityId) {
		try {
			// UPDATE ENTITY SET NAME = ? , JSON_DATA = ?,IS_ACTIVE =?,
			// UPDATED_BY = ?,LAST_MODIFIED = NOW() WHERE ID = ? AND TYPE = ?

			String sQuery = DBUtility.getSQlProperty("UPDATE_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, ds.getName());
			preparedStatement.setString(2, ds.getJsonblob());
			// preparedStatement.setBoolean(3,ds.isActive());
			preparedStatement.setString(3, ds.getUpdatedBy());
			preparedStatement.setInt(4, entityId);
			preparedStatement.setString(5, type);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * This method is to fetch entity details for a given entityId
	 * 
	 * @param type
	 * @param id
	 * @return Entity
	 * @throws SQLException
	 */
	public Entity getEntityById(String type, Integer id) {
		Entity entity = new Entity();

		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_ENTITY_BY_ID");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			preparedStatement.setString(2, type);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created"));
				entity.setUpdatedBy(rs.getString("updated_by"));
				entity.setUpdatedDate(rs.getTimestamp("last_modified"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return entity;
	}

	/**
	 * this method is to delete Entity details
	 * 
	 * @param id
	 */
	public void deleteEntity(Integer id) {
		try {
			String sQuery = DBUtility.getSQlProperty("DELETE_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * DB Call made to fetch the list of particular entity types from DB. Its
	 * mostly used to populate UI drop down components.
	 * 
	 * @param entityType
	 *            {@link String} Type of Entity.
	 * @return {@link List} of Entities of a type.
	 */
	public List<String> listEntity(String entityType) {
		List<String> entities = new ArrayList<String>();
		try {
			String sQuery = DBUtility.getSQlProperty("GET_ENTITY_NAMES");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, entityType);
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				entities.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }
		return entities;
	}

	/**
	 * DB Accessor method queries WHITELIST_CONFIG table to fetch the list of
	 * items that needs to be populated for particular UI component ex-
	 * DataSource Format or DataSource Type etc.
	 * 
	 * @param container
	 *            {@link String} name of the Container
	 * @param name
	 *            {@link String} particular type for given Container.
	 * @return {@link List} of String values
	 */
	public List<String> listConfigurations(String container, String name) {
		List<String> entries = new ArrayList<String>();
		try {
			String sQuery = DBUtility.getSQlProperty("GET_ATTRIBUTE_NAMES");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, container);
			preparedStatement.setString(2, name);
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				entries.add(rs.getString("ENTRY"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }
		return entries;
	}

	/**
	 * This method is to fetch ingestion log details
	 * 
	 * @param type
	 * @param id
	 * @return Entity
	 */
	public List<DataIngestionLog> getIngestionDetailsById(Integer id) {
		List<DataIngestionLog> ingestionLogDtls = new ArrayList<DataIngestionLog>();

		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_INGESTION_LOG");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				DataIngestionLog ingestionLogDtl = new DataIngestionLog();
				ingestionLogDtl.setLogId(rs.getInt("data_ingestion_log_id"));
				ingestionLogDtl.setDataIngestionId(rs
						.getInt("data_ingestion_id"));
				ingestionLogDtl.setBatch(rs.getString("batch"));
				ingestionLogDtl.setStartTime(rs.getTimestamp("job_start_time"));
				// ingestionLogDtl.setEndTime(rs.getDate("job_end_time"));
				ingestionLogDtl.setStage(rs.getString("job_stage"));
				ingestionLogDtl.setStatus(rs.getString("job_status"));
				ingestionLogDtl.setJobMessage(rs.getString("job_msg"));
				ingestionLogDtl.setCreated(rs.getTimestamp("created"));
				ingestionLogDtl.setCreatedBy(rs.getString("created_by"));
				// ingestionLogDtl.setLastModified(rs.getDate("last_modified"));
				// ingestionLogDtl.setUpdatedBy(rs.getString("updated_by"));
				ingestionLogDtls.add(ingestionLogDtl);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return ingestionLogDtls;
	}

	// private void closeConnection(Connection con) {
	// return;
	// /*
	// * try { if(null != con) con.close(); } catch (SQLException e) {
	// * e.printStackTrace(); }
	// */
	//
	// }

	/**
	 * this method is used to list all oozie Stage Status Information.
	 * 
	 * @param type
	 * @return List of OozieStageStatusInfo
	 * @throws SQLException
	 */

	public List<OozieStageStatusInfo> getPipelineStageLogDetailsById(
			Integer pipelineRunId) {

		List<OozieStageStatusInfo> oozieStageStatusInfoList = new ArrayList<OozieStageStatusInfo>();

		String oozieJobId = PipelineJobInfoController.getOozieJobId(String
				.valueOf(pipelineRunId));
		OozieJob oozieJob = PipelineJobInfoController
				.getOozieJobInfo(oozieJobId);

		oozieStageStatusInfoList = oozieJob.getOozieStageStatusInfoList();

		return oozieStageStatusInfoList;
	}

	/**
	 * this method is used to list all successfully processed pipelines
	 * 
	 * @param type
	 * @return List of ProcessedPipeline
	 * @throws SQLException
	 */
	public List<ProcessedPipeline> getProcessedPipelines() {
		List<ProcessedPipeline> processedPipelineList = new ArrayList<ProcessedPipeline>();

		try {
			String sQuery = DBUtility.getSQlProperty("LIST_PIPELINE");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				ProcessedPipeline processedPipeline = new ProcessedPipeline();
				processedPipeline.setName(rs.getString("PIPELINE_NAME"));
				processedPipeline.setDataSet(rs.getString("OUTPUT_DATASET"));
				processedPipelineList.add(processedPipeline);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return processedPipelineList;
	}

	/**
	 * this method is used to list all Machine Learning pipelines
	 * 
	 * @param type
	 * @return List of MLAnalysis
	 * @throws SQLException
	 */

	public List<MLAnalysis> getMLAnalysis() {
		List<MLAnalysis> mlAnalysisList = new ArrayList<MLAnalysis>();

		try {
			String sQuery = DBUtility.getSQlProperty("LIST_MLPIPELINE");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				MLAnalysis mlAnalysis = new MLAnalysis();

				ProcessedPipeline trainingPipeline = new ProcessedPipeline();
				String[] trainingData = rs.getString("training").split("\\|");
				trainingPipeline.setName(trainingData[0]);
				trainingPipeline.setDataSet(trainingData[1]);
				trainingPipeline.setDataType("Training Data");

				ProcessedPipeline testingPipeline = new ProcessedPipeline();
				String[] testingData = rs.getString("testing").split("\\|");
				testingPipeline.setName(testingData[0]);
				testingPipeline.setDataSet(testingData[1]);
				testingPipeline.setDataType("Testing Data");

				mlAnalysis.setMlId(rs.getInt("ml_id"));
				mlAnalysis.setAlgorithm(rs.getString("algorithm"));
				mlAnalysis.setAccuracy(rs.getInt("accuracy"));
				mlAnalysis.setTraining(trainingPipeline);
				mlAnalysis.setTesting(testingPipeline);

				mlAnalysisList.add(mlAnalysis);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return mlAnalysisList;

	}

	/**
	 * this method is used to save ML analysis result
	 * 
	 * @param mlAnalysis
	 * @throws SQLException
	 */

	public void addMLAnalysis(MLAnalysis mlAnalysis) {
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_MLANALYSIS");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, mlAnalysis.getTraining().getName()
					+ "|" + mlAnalysis.getTraining().getDataSet());
			preparedStatement.setString(2, mlAnalysis.getTesting().getName()
					+ "|" + mlAnalysis.getTesting().getDataSet());
			preparedStatement.setString(3, mlAnalysis.getAlgorithm());
			preparedStatement.setInt(4, mlAnalysis.getAccuracy());

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * this method is to delete MLAnalysis details
	 * 
	 * @param id
	 */
	public void deleteMLAnalysis(Integer id) {
		try {
			String sQuery = DBUtility.getSQlProperty("DELETE_MLANALYSIS");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * 
	 * @param ingestionJobId
	 * @return
	 * @author 19217
	 */
	public static String getJobStatus(int ingestionJobId) {
		String jobStatus = "New";
		Connection connection = DBUtility.getConnection();
		String sQuery = DBUtility.getSQlProperty("GET_INGESTION_JOB_STATUS");
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionJobId);
			ResultSet rs = preparedStatement.executeQuery();
			if (rs.next()) {
				jobStatus = rs.getObject(1).toString();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return jobStatus;
	}

	/**
	 * method to get list of source locations of a data source.
	 * 
	 * @return list of data source locations.
	 */
	public static List<String> getSourceLocations() {

		List<String> dataSourceLocationList = new ArrayList<>();

		String sQuery = DBUtility.getSQlProperty("LIST_ENTITY");

		Connection connection = DBUtility.getConnection();
		PreparedStatement preparedStatement;
		try {
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, "DataSource");
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode dataSource = mapper
						.readTree(rs.getString("json_data"));
				JsonNode sourceNode = dataSource.get("location");

				if (sourceNode != null) {
					String sourceLocation = sourceNode.getTextValue();
					if (sourceLocation.endsWith("/")) {
						sourceLocation = sourceLocation.substring(0,
								sourceLocation.length() - 1);
					}
					dataSourceLocationList.add(sourceLocation);
				}
			}

		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		return dataSourceLocationList;
	}

	/**
	 * method to check whether profile name already exist or not
	 * 
	 * @return boolean values true if name is exist else return false.
	 */
	public static boolean getDataschemaName(String name) {

		String sQuery = DBUtility.getSQlProperty("GET_SCHMEA_NAME");

		Connection connection = DBUtility.getConnection();
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, "Dataschema");
			preparedStatement.setString(2, name);
			rs = preparedStatement.executeQuery();

			if (!rs.next()) {
				return false;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * Method retrieves data schema name attributed with the dataset.
	 * 
	 * @param datasetName
	 *            {@link String} name of the dataset.
	 * @return {@link String} Name of the schema
	 */
	public String getSchemaName(String datasetName) {
		String schemaName = "";

		Entity dataset = this.getEntityByName(datasetName);
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(dataset.getJsonblob());
			schemaName = rootNode.get("Schema").getTextValue();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return schemaName;
	}

	/**
	 * Method returns list of column names for a given dataschema
	 * 
	 * @param schema
	 *            {@link String} Name of the schema
	 * @return {@link List} of columns for a given schema
	 */
	public List<String> getColumns(String schema) {
		List<String> columns = new ArrayList<String>();
		Entity dataschema = this.getEntityByName(schema);
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(dataschema.getJsonblob());
			JsonNode colAttrs = rootNode.path("dataAttribute");

			for (JsonNode jsonNode : colAttrs) {
				columns.add(jsonNode.path("Name").getTextValue());
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return columns;
	}

	/**
	 * this method is to delete Entity details
	 * 
	 * @param datasetID
	 *            ,datasourceID,dataschemaID,dataschedularID,destDir,schemaName
	 * @throws SQLException
	 */
	public void moveToArchive(String userName, String dataSetId,
			String dataSourceId, String dataSchemaId, String dataSchedularId,
			String schemaName, String destDir) throws ZeasException,
			SQLException {

		boolean isSuccess = false;
		System.out.println("EntityManager.moveToArchive(): " + userName
				+ " dataSetId: " + dataSetId + " dataSchemaId: " + dataSchemaId
				+ "dataSourceId:" + dataSourceId + " dataSchedularId: "
				+ dataSchedularId + " schemaname: " + schemaName);
		System.out.println("before prepared statement****************");
		PreparedStatement preparedStatement = null;
		System.out.println("before prepared statement****************");
		connection = DBUtility.getConnection();
		System.out.println("after getting connections");
		connection.setAutoCommit(false);
		System.out.println("commit is set to false");
		HashMap<String, String> jsonData = new HashMap<String, String>();
		ResultSet rs = null;
		try {
			System.out.println("getting ingestion details");
			// getting json data of ingestion profile
			String sQuery = DBUtility.getSQlProperty("GET_INGESTION_DETAILS");
			preparedStatement = connection.prepareStatement(sQuery);
			System.out.println(dataSetId + "_____" + dataSourceId + "       "
					+ dataSchemaId);
			preparedStatement.setInt(1, Integer.parseInt(dataSetId));
			preparedStatement.setInt(2, Integer.parseInt(dataSourceId));
			preparedStatement.setInt(3, Integer.parseInt(dataSchemaId));
			preparedStatement.setInt(4, Integer.parseInt(dataSchedularId));
			System.out.println("before executing");
			rs = preparedStatement.executeQuery();
			System.out.println("executecd the query");
			int count = 0;

			while (rs.next()) {
				System.out.println("rs.getMetaData(): " + rs.getMetaData());
				System.out.println(rs.getString(1));
				System.out.println(rs.getString(2));
				jsonData.put(rs.getString("type"), rs.getString("json_data"));
				++count;
				// Get data from the current row and use it
			}
			if (count == 0) {
				System.out.println("No records found");
			}
			System.out.println("No of records: " + count);

			System.out.println("result set is loaded");
		} catch (SQLException ex) {

			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					ex.toString(),
					"Problem in getting ingestion profile from entity table");
		} finally {
			rs.close();
		}

		try {
			System.out.println("copying to archive tableS");
			// insert col1---hm.get(DataSchema) || col2---hm.get( DataSource)
			// json data moving to schema_archive table
			String sQuery1 = DBUtility.getSQlProperty("COPY_TO_ARCHIVE_TABLE");
			preparedStatement = connection.prepareStatement(sQuery1);
			preparedStatement.setInt(1, Integer.parseInt(dataSchemaId));
			preparedStatement.setString(2, schemaName);
			preparedStatement.setString(3, (String) jsonData.get("DataSet"));
			preparedStatement.setString(4, (String) jsonData.get("DataSource"));
			preparedStatement.setString(5, (String) jsonData.get("DataSchema"));
			preparedStatement.setString(6,
					(String) jsonData.get("DataIngestion"));
			preparedStatement.setString(7, userName);
			preparedStatement.executeUpdate();
			System.out.println("delete entity ids");
			// delete all injestion profile data after moving to archive
			String sQuery2 = DBUtility.getSQlProperty("DELETE_ENTITY_IDS");
			preparedStatement = connection.prepareStatement(sQuery2);
			preparedStatement.setInt(1, Integer.parseInt(dataSchemaId));
			preparedStatement.setInt(2, Integer.parseInt(dataSourceId));
			preparedStatement.setInt(3, Integer.parseInt(dataSetId));
			preparedStatement.setInt(4, Integer.parseInt(dataSchedularId));
			preparedStatement.executeUpdate();
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS",
					ConfigurationReader.getProperty("NAMENODE_HOST"));
			FileSystem hdfs = FileSystem.get(conf);
			Path hdfsDirPath = new Path(destDir);
			Path archivePath = new Path(
					ConfigurationReader.getProperty("ARCHIVE_DIR")
							+ File.separator + dataSchemaId);
			if(hdfs.exists(hdfsDirPath)){
			hdfs.copyToLocalFile(hdfsDirPath, archivePath);
			hdfs.delete(hdfsDirPath, true);
			}
			else{System.out.println("HDFS file is not exists still deleting Ingestion profile");}
			connection.commit();
			connection.setAutoCommit(true);
			isSuccess = true;

		} catch (Exception e) {
			// connection.rollback();
			e.printStackTrace();
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					e.toString(),
					"Problem in deleting ingestion profile from entity table");
		} finally {
			if (!isSuccess) {
				connection.rollback();
				connection.setAutoCommit(true);
				throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION,
						"data not  copied",
						"Do not have access to delete file in HDFS");

			}
			if (null != preparedStatement) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			// connection.close();
		}
	}

	/**
	 * Method returns map of archived schema id and name
	 * 
	 * @return HashMap - Contains archived schema name and id
	 * @throws ZeasSQLException
	 */
	public HashMap<String, String> getArchiveProfiles() throws ZeasSQLException {

		HashMap<String, String> map = new HashMap<String, String>();
		PreparedStatement preparedStatement = null;

		try {

			String sQuery = DBUtility.getSQlProperty("LIST_ARCHIVE_PROFILES");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			ResultSet rs = preparedStatement.executeQuery();

			// Get schema id, name and add it into map
			while (rs.next()) {
				String schemaId = Integer.toString(rs.getInt(1));
				String schemaname = rs.getString(2);
				map.put(schemaId, schemaname);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			logger.info("EntityManager.restoreArchivedData(): SQLException: "
					+ e.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					"Processing Request Failed. Refer LOG", "");
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
					preparedStatement = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return map;
	}

	/**
	 * Method returns map of schema id and name
	 * 
	 * @return HashMap - Contains schema name and id
	 * @throws ZeasException
	 */
	public void restoreArchivedData(int schemaId) throws ZeasException {

		boolean isSuccess = false;
		PreparedStatement preparedStatement = null;
		try {

			String sQuery = DBUtility.getSQlProperty("SELECT_ARCHIVE_SCHEMA");
			connection = DBUtility.getConnection();
			connection.setAutoCommit(false);
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, schemaId);
			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				String schemaName = rs.getString("schema_name");
				String schemaJson = rs.getString("schema_json");
				String sourceJson = rs.getString("source_json");
				String datasetJson = rs.getString("dataset_json");
				String schedularJson = rs.getString("schedular_json");
				String userName = rs.getString("user_name");

				addJsonData(schemaName, schemaJson, userName, "DataSchema");
				addJsonData((schemaName + "_Source"), sourceJson, userName,
						"DataSource");
				addJsonData((schemaName + "_DataSet"), datasetJson, userName,
						"DataSet");
				addJsonData((schemaName + "_Schedular"), schedularJson,
						userName, "DataIngestion");

				String archPath = ConfigurationReader
						.getProperty("ARCHIVE_DIR") + File.separator + schemaId;
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS",
						ConfigurationReader.getProperty("NAMENODE_HOST"));
				FileSystem hdfs = FileSystem.get(conf);
				Path hdfsDirPath = new Path(hdfsPath);
				Path archivePath = new Path(archPath);
				hdfs.copyFromLocalFile(archivePath, hdfsDirPath);

				deleteArchivedData(new File(archPath));
				deleteSchemaFromArchive(schemaId);
				connection.commit();
				connection.setAutoCommit(true);
				isSuccess = true;
			}

		} catch (SQLException e) {
			e.printStackTrace();
			logger.info("EntityManager.restoreArchivedData(): SQLException: "
					+ e.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					"Processing Request Failed. Refer LOGS", "");
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("EntityManager.restoreArchivedData(): Exception: "
					+ e.getMessage());
			throw new ZeasException(
					ZeasErrorCode.ZEAS_EXCEPTION,
					"Processing Request Failed. Please check hadoop Configuration. Refer LOGS",
					"");
		} catch (Throwable e) {
			e.printStackTrace();
			logger.info("EntityManager.restoreArchivedData(): Throwable: "
					+ e.getMessage());
			throw new ZeasException(
					ZeasErrorCode.ZEAS_EXCEPTION,
					"Processing Request Failed. Please check hadoop Configuration. Refer LOGS",
					"");
		} finally {
			try {
				if (!isSuccess) {
					connection.rollback();
					connection.setAutoCommit(true);
				}
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Method for getting hdfs path from daset json (from archive data
	 * information)
	 * 
	 * @param schema
	 *            id
	 * @return String - hdfs path
	 * @throws ZeasException
	 */
	public String getHdfsPath(int schemaId) throws ZeasException {
		PreparedStatement preparedStatement = null;
		try {

			String sQuery = DBUtility.getSQlProperty("SELECT_DATASET_JSON");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, schemaId);
			ResultSet rs = preparedStatement.executeQuery();

			if (rs.next()) {
				String datasetJson = rs.getString("dataset_json");
				JSONObject jsonObj = new JSONObject(datasetJson);
				hdfsPath = jsonObj.getString("location");
			}

		} catch (Exception e) {
			logger.info("EntityManager.getHdfsPath(): Exception: "
					+ e.getMessage());
			throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION,
					"Processing Request Failed. Refer LOGS", "");
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
					preparedStatement = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return hdfsPath;
	}

	/**
	 * Method for adding jsons of archived profiles while restoring data
	 * 
	 * @param schemaName
	 * @param schemaJson
	 * @param userName
	 * @param type
	 * @throws ZeasSQLException
	 */
	private void addJsonData(String schemaName, String schemaJson,
			String userName, String type) throws ZeasSQLException {
		Entity entity = new Entity();
		entity.setName(schemaName);
		entity.setType(type);
		entity.setJsonblob(schemaJson);
		entity.setCreatedBy(userName);
		entity.setUpdatedBy(userName);
		entity.setActive(true);

		addEntity(entity);

	}

	/**
	 * Method for deleting schema from archive table once the data is restored
	 * to hdfs location
	 * 
	 * @param schemaId
	 * @throws ZeasSQLException
	 */
	private void deleteSchemaFromArchive(int schemaId) throws ZeasSQLException {

		PreparedStatement preparedStatement = null;
		try {

			String sQuery = DBUtility.getSQlProperty("DELETE_ARCHIVE_SCHEMA");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, schemaId);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			logger.info("EntityManager.deleteSchemaFromArchive(): SQLException: "
					+ e.getMessage());
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
					preparedStatement = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Method for deleting the data from local archived folder once restoring of
	 * data is done
	 * 
	 * @param dir
	 *            - folder to be deleted
	 * @throws ZeasException
	 */
	private boolean deleteArchivedData(File dir) throws ZeasException {

		boolean deletedStatus = false;

		try {

			if (dir.isDirectory()) {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					boolean success = deleteArchivedData(new File(dir,
							children[i]));
					if (!success) {
						return false;
					}
				}
			}

			deletedStatus = dir.delete();
		} catch (Exception e) {
			logger.info("EntityManager.deleteSchemaFromArchive(): Exception: "
					+ e.getMessage());
		}

		return deletedStatus;
	}
}