package com.itc.zeas.ingestion.automatic.bulk;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.util.ajax.JSON;
import org.jdom2.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.ingestion.automatic.rdbms.SqoopImportDetails;
import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.model.BulkEntity;
import com.itc.zeas.profile.model.BulkProfile;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.usermanagement.model.UserLevelPermission;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.CommonUtils;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.utility.utility.ConfigurationReader;
import com.itc.zeas.validation.rule.JSONDataParser;
import com.mysql.jdbc.Statement;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.pipeline.WorkflowBuilder;
import com.zdp.dao.ZDPDataAccessObjectImpl;



public class BulkEntityManager implements EntityManagerInterface {
	private static final Logger LOGGER = Logger.getLogger(BulkEntityManager.class);
	ZDPDataAccessObjectImpl accessObjectActivity = new ZDPDataAccessObjectImpl();
	SqoopImportDetails sqoop = new SqoopImportDetails();

	@Override
	public List<Entity> getEntity(String type, HttpServletRequest httpRequest) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addEntity(Entity entity, HttpServletRequest request) throws Exception {
		Map<String, JSONObject> jobID = new HashMap<String, JSONObject>();
		Map<String, JSONObject> jobIDForSQL = new HashMap<String, JSONObject>();

		JSONArray arrayOfJsons = jsonArray(entity);
		ResultSet rs;
		long Bulk_Entity_Id = 0;
		CommonUtils commonUtils = new CommonUtils();
		String accessToken = commonUtils.extractAuthTokenFromRequest(request);
		String userName = commonUtils.getUserNameFromToken(accessToken);
		Connection connectionEntity = null;
		Connection connectionId = null;
		Connection connectionBulkEntity = null;
		PreparedStatement preparedStatementForEntity = null;
		PreparedStatement preparedStatementForBulkEntity = null;
		PreparedStatement preparedStatementForID = null;
		try {

			// adding to entity table
			String sQueryForEntity = ConnectionUtility.getSQlProperty("INSERT_ENTITY");
			// String entityIdQuery = DBUtility.getSQlProperty("GET_ENTITY_ID");
			connectionEntity = ConnectionUtility.getConnection();
			preparedStatementForEntity = connectionEntity.prepareStatement(sQueryForEntity,
					Statement.RETURN_GENERATED_KEYS);
			preparedStatementForEntity.setString(1, entity.getName());
			preparedStatementForEntity.setString(2, entity.getType());
			preparedStatementForEntity.setString(3, "");
			// Currently sending '1' as isActive value
			preparedStatementForEntity.setBoolean(4, true);
			preparedStatementForEntity.setString(5, entity.getCreatedBy());
			preparedStatementForEntity.setString(6, entity.getUpdatedBy());
			preparedStatementForEntity.executeUpdate();
			ResultSet resultSetKey = preparedStatementForEntity.getGeneratedKeys();

			if (resultSetKey.next()) {
				Bulk_Entity_Id = resultSetKey.getLong(1);
			}

			// adding to bulk_entity table
			String sQueryForBulkEntity = ConnectionUtility.getSQlProperty("INSERT_BULK_ENTITY");
			preparedStatementForBulkEntity = connectionEntity.prepareStatement(sQueryForBulkEntity);

			for (int i = 0; i < arrayOfJsons.length(); i++) {

				JSONObject jsonobj = arrayOfJsons.getJSONObject(i);
				JSONObject info = jsonobj.getJSONObject("info");
				preparedStatementForBulkEntity.setString(1, info.getString("BULK INGESTION ID"));
				preparedStatementForBulkEntity.setLong(2, Bulk_Entity_Id);
				preparedStatementForBulkEntity.setString(3, info.getString("NAME"));
				preparedStatementForBulkEntity.setString(4, info.getString("JOB NAME"));
				preparedStatementForBulkEntity.setString(5, jsonobj.getJSONObject("jsonobjectForSchema").toString());
				preparedStatementForBulkEntity.setString(6, jsonobj.getJSONObject("jsonobjectForSource").toString());
				preparedStatementForBulkEntity.setString(7, jsonobj.getJSONObject("jsonobjectForDataSet").toString());
				preparedStatementForBulkEntity.setBoolean(8, true);
				preparedStatementForBulkEntity.setString(9, userName);
				preparedStatementForBulkEntity.setString(10, userName);
				preparedStatementForBulkEntity.addBatch();
				JSONObject sourceTypeJSONObj = jsonobj.getJSONObject("jsonobjectForSource");
				JSONObject jsonSource = sourceTypeJSONObj.getJSONObject("fileData");
				String source = jsonSource.getString("SOURCE");
				if (source.equalsIgnoreCase("File")) {
					jobID.put(info.getString("JOB NAME"), jsonobj.getJSONObject("jsonobjectForSchema"));
				} else {
					jobIDForSQL.put(info.getString("JOB NAME"), jsonobj.getJSONObject("jsonobjectForSchema"));
				}
			}
			preparedStatementForBulkEntity.executeBatch();
		} catch (SQLException e) {
			LOGGER.info("getColumnValues: SQLException: " + e.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, e.getMessage(), "");
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatementForEntity, connectionEntity);
			ConnectionUtility.releaseConnectionResources(preparedStatementForID, connectionId);
			ConnectionUtility.releaseConnectionResources(preparedStatementForBulkEntity, connectionBulkEntity);

		}

		String statusInfo = "New Bulk Ingestion Profile '" + entity.getName() + "' created by " + entity.getCreatedBy();
		List<String> users = new ArrayList<String>();
		users.add(entity.getCreatedBy());
		accessObjectActivity.addActivitiesBatchForNewAPI(entity.getName(), statusInfo, "ingestion", "SUCCESS", users,
				entity.getCreatedBy());
		// calling a oozie method for each jobProfile
		bulkFileIngestion(jobID, entity.getName(), userName, "FILE");
		bulkFileIngestion(jobIDForSQL, entity.getName(), userName, "RDBMS");

	}

	public static void main(String args[]) throws ZeasSQLException, JsonProcessingException, IOException,
			ParseException, org.json.simple.parser.ParseException {

		/*
		 * WorkflowBuilder wf = new WorkflowBuilder(); Document doc =
		 * wf.getOozieWorkFlowTemplateForBulkFileIngestion("java-action",
		 * "java-action", "workflowName", "FILE");
		 * wf.saveWorkFlowXML(doc,"D:\\workflow.xml");
		 */

		/*
		 * String[] args1 = new String[2]; args1[0] = ShellScriptExecutor.BASH;
		 * args1[1] = /home/zeas/zeas/Profiles/bulkDemo4/Job1091/run.sh;
		 * 
		 * ShellScriptExecutor shExe = new ShellScriptExecutor();
		 * shExe.runScript(args);
		 */
		BulkEntityManager bem = new BulkEntityManager();
		String endDate = bem.endDate("2017-04-24");
		System.out.println(endDate);

	}

	// method for generation of folder for each job
	@SuppressWarnings("null")
	private void bulkFileIngestion(Map<String, JSONObject> jobID, String profileName, String userName,
			String SourceType) throws Exception {
		
		Iterator it = jobID.entrySet().iterator();
		while (it.hasNext()) {
			WorkflowBuilder wf = new WorkflowBuilder();
			BulkControllerUtility bcu = new BulkControllerUtility();

			Map.Entry pair = (Map.Entry) it.next();
			System.out.println(pair.getKey() + " = " + pair.getValue());
			String jobName = (String) pair.getKey();
			JSONObject value = (JSONObject) pair.getValue();
			JSONObject fileData = value.getJSONObject("fileData");
			String startTime = fileData.getString("EXECUTION SCHEDULE");
			String inputSourcePath;
			String workflowName = profileName + "_" + jobName;

			String sourceType = fileData.getString("SOURCE TYPE");
			String schema = fileData.getString("SCHEMA");
			// APP_PATH =/home/zeas/zeas/userName/Profiles/" +
			// profileName/+jobName
			// HDFS_PATH=/user/zeas/userName/Profiles/" + profileName/+jobName
			String APP_PATH = System.getProperty("user.home") + "/zeas/" + userName + "/Profiles/" + profileName + "/"
					+ jobName;
			String HDFS_PATH = ConfigurationReader.getProperty("HDFS_USER_PATH") + userName + "/Profiles/" + profileName
					+ "/" + jobName;
			System.out.println(APP_PATH);
			File dir = new File(APP_PATH);
			if (!dir.exists()) {
				dir.mkdirs();
				System.out.println("successfully created");
			}

			String endTime = endDate(startTime);
			startTime = startTime + "T00:01Z";
			endTime = endTime + "T00:01Z";
			String header = fileData.getString("HEADER");
			if (header.equalsIgnoreCase("Y")) {
				header = "true";
			} else {
				header = "false";
			}

			// lib directory

			String lib = "lib";
			String libPath = APP_PATH + "/" + lib;
			File libdir = new File(libPath);
			if (!libdir.exists()) {
				libdir.mkdirs();
				System.out.println("successfully created");
			}

			String cmd[] = { "/home/zeas/zeas/script/copyLib.sh", APP_PATH + "/lib" };
			ShellScriptExecutor.runScript(cmd);
			bcu.executeHDFSPUTCommand("lib", APP_PATH, profileName, jobName, userName);

			// hive.sh file creation and saving to HDFS
			String tableName = jobName + "_Dataset";
			EntityManager entityManager = new EntityManager();
			BulkEntity bulkEntity = new BulkEntity();
			bulkEntity = entityManager.getBulkEntityByName(jobName);
			JSONDataParser dataTypeparser = new JSONDataParser();
			Map<String, String> columnNameAndDataType = dataTypeparser.JsonParser(bulkEntity.getJsonblob());
			StringBuilder schemaString = new StringBuilder();

			for (Map.Entry<String, String> entry : columnNameAndDataType.entrySet()) {
				schemaString.append(entry.getKey());
				schemaString.append(" ");
				switch (entry.getValue()) {
				case "long":
					schemaString.append("BIGINT");
					break;
				case "varchar":
					schemaString.append("String");
					break;
				default:
					schemaString.append(entry.getValue());
				}
				schemaString.append(",");

			}
			schemaString.deleteCharAt(schemaString.lastIndexOf(","));

			if (SourceType.equalsIgnoreCase("FILE")) {
				inputSourcePath = fileData.getString("FULL FILEPATH AT SOURCE");
				// input
				String input = "input";
				String inputPath = APP_PATH + "/" + input;
				File inputdir = new File(inputPath);
				if (!inputdir.exists()) {
					inputdir.mkdirs();
				}
				String inputCopy[] = { "/home/zeas/zeas/script/inputCopy.sh", inputSourcePath, inputPath };
				ShellScriptExecutor.runScript(inputCopy);
				bcu.executeHDFSPUTCommand("input", APP_PATH, profileName, jobName, userName);

				// workflow.xml for FILE
				Document doc = wf.getOozieWorkFlowTemplateForBulkFileIngestion("java-action", "java-action",
						workflowName, "FILE", HDFS_PATH + "/lib");
				wf.saveWorkFlowXML(doc, APP_PATH + "/workflow.xml");
				bcu.executeHDFSPUTCommand("workflow.xml", APP_PATH, profileName, jobName, userName);

				
			}

			if (SourceType.equalsIgnoreCase("RDBMS")) {

				sqoop.getDetailsForBulkSQLImport(APP_PATH, jobName);

				String SQOOP_APP_PATH = APP_PATH;

				bcu.executeHDFSPUTCommand("SchedulerSqoopImport.sh", SQOOP_APP_PATH, profileName, jobName, userName);

				// workflow.xml for RDBMS
				Document doc = wf.getOozieWorkFlowTemplateForSQLBulkIngestion("sqoop", "sqoop", workflowName, "rdbms");
				wf.saveWorkFlowXML(doc, APP_PATH + "/workflow.xml");
				bcu.executeHDFSPUTCommand("workflow.xml", APP_PATH, profileName, jobName, userName);

			}
			// hive.hql
			String path = bcu.hiveShellScript(schemaString.toString(), tableName, profileName, jobName, userName,
					sourceType);
			bcu.executeHDFSPUTCommand("hive.hql", path, profileName, jobName, userName);
			bcu.executeHDFSPUTCommand("hive-site.xml", System.getProperty("user.home") + "/zeas/Config/",
					profileName, jobName, userName);


			Document coordinatorDoc = wf.getcoordinatorTemplate(HDFS_PATH, "${coord:days(1)}", workflowName);
			wf.saveWorkFlowXML(coordinatorDoc, APP_PATH + "/coordinator.xml");
			bcu.executeHDFSPUTCommand("coordinator.xml", APP_PATH, profileName, jobName, userName);

			bcu.coordinatorConfigProperties(startTime, endTime, "${coord:days(1)}", jobName, profileName, "GMT",
					jobName, userName, sourceType, schema, header);

			// run.sh
			String runFile = "run.sh";
			String filePath = APP_PATH + "/" + runFile;
			File file = new File(filePath);
			if (!file.exists()) {
				file.createNewFile();
				file.setExecutable(true);
				file.setReadable(true);
				file.setWritable(true);

			}
			File file1 = new File(filePath);
			FileWriter fw = new FileWriter(file1);
			BufferedWriter bw = new BufferedWriter(fw);
			StringBuilder builder = new StringBuilder();
			builder.append("#!/bin/bash");
			builder.append("\n");
			builder.append("oozie job --oozie " + ConfigurationReader.getProperty("OOZIE_ENGINE") + " -config "
					+ APP_PATH + "/coordinator.properties -run");
			bw.write(builder.toString());
			bw.flush();

			String[] args = new String[2];
			args[0] = ShellScriptExecutor.BASH;
			args[1] = filePath;
			ShellScriptExecutor.runScript(args);

			// hive.sh
			/*if (SourceType.equalsIgnoreCase("RDBMS")) {
				String runHiveFile = "hive.sh";
				String hiveFilePath = APP_PATH + "/" + runHiveFile;
				File hiveFile = new File(hiveFilePath);
				if (!hiveFile.exists()) {
					hiveFile.createNewFile();
					hiveFile.setExecutable(true);
					hiveFile.setReadable(true);
					hiveFile.setWritable(true);

				}
				File hfile = new File(hiveFilePath);
				FileWriter hfw = new FileWriter(hfile);
				BufferedWriter hbw = new BufferedWriter(hfw);
				StringBuilder hivebuilder = new StringBuilder();
				hivebuilder.append("#!/bin/bash");
				hivebuilder.append("\n");
				hivebuilder.append("hive -f " + APP_PATH + "/hive.hql");
				hbw.write(hivebuilder.toString());
				hbw.flush();

				String[] hiveargs = new String[2];
				args[0] = ShellScriptExecutor.BASH;
				args[1] = hiveFilePath;
				ShellScriptExecutor.runScript(hiveargs);
			}
*/
		}

	}

	private String endDate(String startTime) throws ParseException {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(startTime));
		c.add(Calendar.DATE, 1); // number of days to add
		String endDate = sdf.format(c.getTime());
		return endDate;

	}

	@SuppressWarnings("null")
	public JSONArray jsonArray(Entity entity) throws ZeasException {
		List<List<Object>> excelData = entity.getJsonblobForBulk();
		String profileName = entity.getName();
		JSONArray array = parse(excelData);
		/*
		 * Iterator itr = excelData.iterator(); List<Object> headers =
		 * (List<Object>) itr.next();
		 */

		JSONArray arrayOfJsons = new JSONArray();
		String[] type = new String[3];
		type[0] = "DataSchema";
		type[1] = "DataSource";
		type[2] = "DataSet";

		try {
			for (int j = 0; j < array.length(); j++) {

				JSONObject arrayForEachJob = new JSONObject();
				JSONObject jsonobj = new JSONObject();
				JSONObject jsonFromExisting = array.getJSONObject(j);

				jsonobj.put("BULK INGESTION ID", jsonFromExisting.get("BULK INGESTION ID"));
				jsonobj.put("NAME", profileName);
				jsonobj.put("JOB NAME", jsonFromExisting.get("JOB NAME"));
				arrayForEachJob.put("info", jsonobj);

				for (int i = 0; i < type.length; i++) {
					switch (type[i]) {
					case "DataSchema":
						JSONObject jsonobjectForSchema = new JSONObject();

						jsonobjectForSchema.put("name", jsonFromExisting.get("JOB NAME"));
						jsonobjectForSchema.put("type", "DataSchema");
						// jsonobjectForSchema.put("dataAttribute", headers);
						String str = (String) jsonFromExisting.get("SCHEMA");
						int k = str.indexOf(":");
						System.out.println(k);
						String a = str.substring(k + 1);
						System.out.println(a);
						jsonobjectForSchema.put("dataAttribute", JSON.parse(a));
						jsonobjectForSchema.put("dataSchemaType", "Automatic");
						jsonobjectForSchema.put("fileData", jsonFromExisting);
						arrayForEachJob.put("jsonobjectForSchema", jsonobjectForSchema);
						break;

					case "DataSource":

						JSONObject jsonobjectForSource = new JSONObject();
						jsonobjectForSource.put("format", jsonFromExisting.get("SOURCE TYPE"));
						jsonobjectForSource.put("schema", jsonFromExisting.get("JOB NAME"));
						// jsonobjectForSource.put("dataAttribute",
						// JSON.parse((String) jsonFromExisting.get("SCHEMA")));
						String str1 = (String) jsonFromExisting.get("SCHEMA");
						int k1 = str1.indexOf(":");
						System.out.println(k1);
						String a1 = str1.substring(k1 + 1);
						System.out.println(a1);
						jsonobjectForSource.put("dataAttribute", JSON.parse(a1));
						jsonobjectForSource.put("fileData", jsonFromExisting);
						jsonobjectForSource.put("dataSource", jsonFromExisting.get("JOB NAME") + "_Source");
						jsonobjectForSource.put("name", jsonFromExisting.get("JOB NAME") + "_Source");
						jsonobjectForSource.put("dataSourcerId", jsonFromExisting.get("JOB NAME") + "_Source");
						jsonobjectForSource.put("sourcerType", jsonFromExisting.get("SOURCE TYPE"));
						arrayForEachJob.put("jsonobjectForSource", jsonobjectForSource);
						break;

					case "DataSet":

						JSONObject jsonobjectForDataSet = new JSONObject();
						jsonobjectForDataSet.put("dataIngestionId", jsonFromExisting.get("JOB NAME") + "_Dataset");
						jsonobjectForDataSet.put("name", jsonFromExisting.get("JOB NAME") + "_Dataset");
						jsonobjectForDataSet.put("Schema", jsonFromExisting.get("JOB NAME"));
						jsonobjectForDataSet.put("location", "/user/zeas/" + entity.getCreatedBy() + "/Profiles/"
								+ entity.getName() + "/" + jsonFromExisting.get("JOB NAME"));
						arrayForEachJob.put("jsonobjectForDataSet", jsonobjectForDataSet);
						break;
					}
				}
				arrayOfJsons.put(arrayForEachJob);
			}

		} catch (JSONException e) {
			LOGGER.info("JSON exception for type" + e.getMessage());
			e.printStackTrace();

		} catch (NullPointerException e) {
			LOGGER.info(e.getMessage());
			e.printStackTrace();
		}
		return arrayOfJsons;
	}

	@SuppressWarnings("null")
	public JSONArray parse(List<List<Object>> excelData) {
		List<Object> headers;
		List<Object> data;

		Iterator itr = excelData.iterator();
		headers = (List<Object>) itr.next();
		JSONArray array = new JSONArray();
		JSONObject json = null;
		// getting each row
		while (itr.hasNext()) {
			data = (List<Object>) itr.next();
			// looping for headers
			for (int i = 0; i < headers.size();) {
				json = new JSONObject();
				// looping in each row
				for (int j = 0; j < data.size(); j++) {
					if (i < headers.size()) {
						if (headers.get(i).toString().equalsIgnoreCase("schema")) {

							json.put(headers.get(i).toString().toUpperCase(), JSON.parse((String) data.get(j)));
						}
						json.put(headers.get(i).toString().toUpperCase(), data.get(j));
						i++;
					}
				}

			}
			array.put(json);
		}
		System.out.println(array);
		return array;
	}

	public List<BulkProfile> getBulkProfiles(HttpServletRequest httpServletRequest, String name) throws Exception {

		String profileName = name;
		CommonUtils commonUtils = new CommonUtils();
		String accessToken = commonUtils.extractAuthTokenFromRequest(httpServletRequest);
		String userId = commonUtils.getUserNameFromToken(accessToken);
		ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
		Boolean isSuperUser = zdpUserAccess.isSuperUser(userId);
		if (isSuperUser) {
			LOGGER.debug("user is super user");
			return getProfilesForAdminUser(profileName);
		}
		LOGGER.info("Getting user details for listing datasets");
		List<BulkProfile> profileList = new ArrayList<BulkProfile>();

		String sQuery;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);

			String userNameList = "'" + userId + "'";
			for (String username : userNamePermissionMap.keySet()) {
				userNameList = userNameList + ",'" + username + "'";
			}

			UserLevelPermission userLevelPermission = zdpUserAccess.getUserLevelPermission(userId);
			int userLevelDatasetPermission = userLevelPermission.getDatasetPermission();

			connection = ConnectionUtility.getConnection();
			sQuery = "select * from bulk_entity where CREATED_BY in(" + userNameList + ") and name=" + "'" + profileName
					+ "'";
			preparedStatement = connection.prepareStatement(sQuery);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				BulkProfile bulkProfile = new BulkProfile();
				int id = rs.getInt("id");
				bulkProfile.setId(id);
				String createdby = rs.getString("CREATED_BY");
				bulkProfile.setCreatedby(createdby);
				if (createdby.equals(userId)) {
					// creator is requesting user
					bulkProfile.setPermissionLevel(userLevelDatasetPermission);
				} else {
					Integer groupLevelpermission = userNamePermissionMap.get(createdby);
					bulkProfile.setPermissionLevel((groupLevelpermission & userLevelDatasetPermission));
				}
				String jsonBlob = rs.getString("json_data_source");
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(jsonBlob);
				String jobId = (rootNode.get("schema")).getTextValue();
				String source = (rootNode.get("dataSource").getTextValue());
				bulkProfile.setJobId(jobId);
				bulkProfile.setSource(source);
				String sourceType = (rootNode.get("fileData").findValue("SOURCE").getTextValue());
				bulkProfile.setSourceType(sourceType);
				String requestedDate = (rootNode.get("fileData").findValue("EXECUTION SCHEDULE").getTextValue());
				bulkProfile.setRequestedDate(requestedDate);
				String jsonBlobForDataSet = rs.getString("json_data_dataset");
				ObjectMapper mapperDataset = new ObjectMapper();
				JsonNode rootNodeDataSet = mapperDataset.readTree(jsonBlobForDataSet);
				String dataset = (rootNodeDataSet.get("location")).getTextValue();
				bulkProfile.setDataset(dataset);
				profileList.add(bulkProfile);

			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.getMessage();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}

		List<BulkProfile> profileListToBeSorted = new ArrayList<BulkProfile>();
		List<BulkProfile> profileListToBeAppended = new ArrayList<BulkProfile>();
		for (BulkProfile bulkProfile : profileList) {
			if (bulkProfile.getLastModifiedDate() != null) {
				profileListToBeSorted.add(bulkProfile);
			} else {
				profileListToBeAppended.add(bulkProfile);
			}
		}
		for (BulkProfile bulkProfile : profileListToBeAppended) {
			profileListToBeSorted.add(bulkProfile);
		}
		return profileListToBeSorted;
	}

	private List<BulkProfile> getProfilesForAdminUser(String profileName)
			throws ZeasSQLException, JsonProcessingException, IOException {

		List<BulkProfile> profileList = new ArrayList<BulkProfile>();
		String sQuery;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			connection = ConnectionUtility.getConnection();
			// sQuery = ConnectionUtility.getSQlProperty("LIST_ENTITY_ADMIN");
			sQuery = "select * from zeas.bulk_entity where name=" + "'" + profileName + "'";
			preparedStatement = connection.prepareStatement(sQuery);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				BulkProfile bulkProfile = new BulkProfile();
				int id = rs.getInt("id");
				bulkProfile.setId(id);
				String createdby = rs.getString("CREATED_BY");
				bulkProfile.setCreatedby(createdby);
				String jsonBlob = rs.getString("json_data_source");
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(jsonBlob);
				String jobId = (rootNode.get("schema")).getTextValue();
				String source = (rootNode.get("dataSource").getTextValue());
				bulkProfile.setJobId(jobId);
				bulkProfile.setSource(source);
				String sourceType = (rootNode.get("fileData").findValue("SOURCE").getTextValue());
				bulkProfile.setSourceType(sourceType);
				String requestedDate = (rootNode.get("fileData").findValue("EXECUTION SCHEDULE").getTextValue());
				bulkProfile.setRequestedDate(requestedDate);

				String jsonBlobForDataSet = rs.getString("json_data_dataset");
				ObjectMapper mapperDataset = new ObjectMapper();
				JsonNode rootNodeDataSet = mapperDataset.readTree(jsonBlobForDataSet);
				String dataset = (rootNodeDataSet.get("location")).getTextValue();
				bulkProfile.setDataset(dataset);
				profileList.add(bulkProfile);

			}
		} catch (SQLException e) {
			LOGGER.info("getColumnValues: SQLException: " + e.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, e.getMessage(), "");
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		// finally {
		// closeConnection(connection);
		// }
		// Collections.sort(profileList);
		List<BulkProfile> profileListToBeSorted = new ArrayList<BulkProfile>();
		List<BulkProfile> profileListToBeAppended = new ArrayList<BulkProfile>();
		for (BulkProfile bulkProfile : profileList) {
			if (bulkProfile.getLastModifiedDate() != null) {
				profileListToBeSorted.add(bulkProfile);
			} else {
				profileListToBeAppended.add(bulkProfile);
			}
		}
		// Collections.sort(profileListToBeSorted);
		for (BulkProfile bulkProfile : profileListToBeAppended) {
			profileListToBeSorted.add(bulkProfile);
		}
		System.out.println(profileListToBeSorted);
		return profileListToBeSorted;
	}

	public Entity validation(Entity entity) {
		List<List<Object>> jsondata = entity.getJsonblobForBulk();
		List<String> bulkNames = entity.getBulkNames();
		List<String> existingBulkNames = new ArrayList<String>();
		boolean isExist = false;
		for (String name : bulkNames) {
			try {
				isExist = EntityManager.getBulkName(name.toUpperCase());
				if (isExist) {
					existingBulkNames.add(name);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		entity.setBulkNames(existingBulkNames);
		return entity;
	}

}
