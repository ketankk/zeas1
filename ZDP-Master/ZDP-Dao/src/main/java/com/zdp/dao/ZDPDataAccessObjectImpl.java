package com.zdp.dao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.zeas.ingestion.model.ZDPRunLogDetails;
import com.itc.zeas.ingestion.model.ZDPScheduler;
import com.itc.zeas.model.ModuleSchema;
import com.itc.zeas.model.ProjectRunHistory;
import com.itc.zeas.project.extras.QueryConstants;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.project.model.HiveSchema;
import com.itc.zeas.project.model.ProjectEntity;
import com.itc.zeas.project.model.SearchCriterion;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.utility.utility.ZDPDaoUtility;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.StringUtils;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;

public class ZDPDataAccessObjectImpl implements ZDPDataAccessObject {

	public static Logger logger = Logger.getLogger(ZDPDataAccessObjectImpl.class);

	@Override
	public ProjectEntity addEntity(ProjectEntity projectEntity) {
		String entityType = projectEntity.getSchemaType();
		Long id = Long.valueOf(projectEntity.getId());
		// workspac table create auto generate id
		if (id == 0 && !(entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_WORKSPACE)
				|| entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_PROJECT_HISTORY)
				|| entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_MODULE_HISTORY))) {
			id = ZDPDaoUtility.getSequenceId("");
			projectEntity.setId(id);
		} else if (entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_PROJECT_HISTORY)
				|| entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_MODULE_HISTORY)) {
			id = ZDPDaoUtility.getSequenceId(ZDPDaoConstant.HISTORY);
			projectEntity.setId(id);
		}

		if (projectEntity.getName() != null && projectEntity.getName().equalsIgnoreCase("Hive") && !projectEntity.isOutputDefined()) {
			updateJsonForColumnList(projectEntity);
		}
		String tableName = ZDPDaoUtility.getTableName(entityType);
		Map<String, String> columnAndType = ZDPDaoUtility.getColumnAndType(entityType, projectEntity);
		Map<String, String> columnAndValue = ZDPDaoUtility.getColumnAndValue(entityType, projectEntity);

		boolean isInserted = executeCreateQuery(columnAndType, columnAndValue, tableName);
		if (!(entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_PROJECT_HISTORY)
				|| entityType.equalsIgnoreCase(ZDPDaoConstant.ZDP_MODULE_HISTORY))) {
			if(projectEntity.getVersion() == null)
			{
				projectEntity.setVersion("1");
			}
			else{
			projectEntity.setVersion(String.valueOf(Integer.parseInt(projectEntity.getVersion()) + 1));
			}
		}
		if (!isInserted) {
			System.out.println("project not inserted");
			return null;
		}
		return projectEntity;
	}

	@Override
	public ProjectEntity updateEntity(ProjectEntity projectEntity, String type, Integer id) {
		return null;
	}

	public Boolean updateObject(String type, Map<String, String> columnNameAndValues, List<SearchCriterion> criterions)
			throws Exception {
		logger.info("updateObject api started..");
		logger.info("type:" + type);
		logger.info("columnNameAndValues :" + columnNameAndValues.toString());
		boolean isUpdatable = false;
		String tableName = ZDPDaoUtility.getTableName(type);
		logger.info("table name :" + tableName);
		WhereClauseQueryFormatioin queryFormatioin = new WhereClauseQueryFormatioin();
		Map<String, List<String>> queryAndValues = queryFormatioin.getWhereClause(criterions);
		String updatedColumnValue = getUpdateValue(columnNameAndValues);
		logger.info("updatedColumnValue :" + updatedColumnValue);
		String query = "update " + tableName + " " + updatedColumnValue + " "
				+ queryAndValues.get(QueryConstants.columnQuery).get(0) + " ";
		logger.info("query :" + query);
		if (columnNameAndValues != null && columnNameAndValues.size() > 0) {
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = ConnectionUtility.getConnection();
				ps = conn.prepareStatement(query);
				int index = 1;
				for (String value : queryAndValues.get(QueryConstants.columnValues)) {
					ps.setString(index, value);
					index++;
				}
				int count = ps.executeUpdate();
				if (count > 0) {
					isUpdatable = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				ConnectionUtility.releaseConnectionResources(ps, conn);
			}
		} else {
			System.out.println("there is no column value to update the record in table");
			return null;
		}
		return isUpdatable;
	}

	private String getUpdateValue(Map<String, String> columnNameAndValues) {

		StringBuilder builder = new StringBuilder();
		if (columnNameAndValues != null && columnNameAndValues.size() > 0) {
			builder.append(" set ");
			int columnSize = columnNameAndValues.size();
			int index = 1;
			for (Entry<String, String> entry : columnNameAndValues.entrySet()) {

				builder.append(" " + entry.getKey() + "='" + entry.getValue() + "' ");
				if (index < columnSize) {
					builder.append(",");
					index++;
				}
			}
		}

		return builder.toString();
	}

	@Override
	public boolean deleteEntity(String comp_type, List<SearchCriterion> criterions) throws Exception {

		String tableName = ZDPDaoUtility.getTableName(comp_type);
		WhereClauseQueryFormatioin queryFormatioin = new WhereClauseQueryFormatioin();
		Map<String, List<String>> queryAndValues = queryFormatioin.getWhereClause(criterions);
		boolean isSuccess = false;
		String query = "delete from " + tableName + " " + queryAndValues.get(QueryConstants.columnQuery).get(0) + " ";
		System.out.println("query:" + query);
		System.out.println("values :" + queryAndValues.get(QueryConstants.columnValues));
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(query);
			int index = 1;
			for (String value : queryAndValues.get(QueryConstants.columnValues)) {
				ps.setString(index, value);
				index++;
			}
			int count = ps.executeUpdate();
			if (count > 0) {
				isSuccess = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isSuccess;
	}

	private boolean executeCreateQuery(Map<String, String> columnAndType, Map<String, String> columnAndValue,
			String tableName) {

		boolean isInserted = false;
		String query = ZDPDaoUtility.formCreateQuery(tableName, columnAndType);
		boolean isVersionIncrease = true;
		if (tableName.equalsIgnoreCase(ZDPDaoConstant.ZDP_PROJECT_HISTORY)
				|| tableName.equalsIgnoreCase(ZDPDaoConstant.ZDP_MODULE_HISTORY)) {
			isVersionIncrease = false;
		}
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();

			ps = conn.prepareStatement(query);
			ps = getPreparedStatement(columnAndType, columnAndValue, ps, isVersionIncrease);
			int record = ps.executeUpdate();
			if (record > 0)
				isInserted = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isInserted;
	}

	private PreparedStatement getPreparedStatement(Map<String, String> columnAndType,
			Map<String, String> columnAndValue, PreparedStatement ps, boolean isVersionIncrease) throws SQLException {

		int index = 1;
		for (Entry<String, String> entry : columnAndType.entrySet()) {
			String val = entry.getValue();
			String key = entry.getKey();
			String columnName = val.toLowerCase();
			switch (columnName) {

			case "string":
				ps.setString(index, columnAndValue.get(key));
				break;

			case "int":
			case "integer":
				String value = columnAndValue.get(key);
				int version = 0;
				if (value != null) {
					try {
						version = Integer.parseInt(value);
					} catch (NumberFormatException e) {
					}
				}
				if (key.equalsIgnoreCase("version") && isVersionIncrease) {

					version = version + 1;
				}
				ps.setInt(index, version);
				break;

			case "long":
			case "Long":
				String longValue = columnAndValue.get(key);
				long lVal = 0;
				if (longValue != null) {
					try {
						lVal = Long.parseLong(longValue);
					} catch (NumberFormatException e) {
					}
				}
				ps.setLong(index, lVal);
				break;

			case "timestamp":
				java.util.Date date = new java.util.Date();
				ps.setTimestamp(index, new Timestamp(date.getTime()));
				break;

			}
			index++;
		}
		return ps;
	}

	@Override
	public ProjectEntity findExactObject(String comp_type, List<SearchCriterion> criterions) throws Exception {

		ProjectEntity projectEntity = null;
		System.out.println("comp_name :" + comp_type);
		String tableName = ZDPDaoUtility.getTableName(comp_type);
		System.out.println("table*********:" + tableName);
		WhereClauseQueryFormatioin queryFormatioin = new WhereClauseQueryFormatioin();
		Map<String, List<String>> queryAndValues = queryFormatioin.getWhereClause(criterions);
		String query = "select *from " + tableName + " " + queryAndValues.get(QueryConstants.columnQuery).get(0) + " ";
		System.out.println("query:" + query);
		System.out.println("values :" + queryAndValues.get(QueryConstants.columnValues));
		Connection conn = ConnectionUtility.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(query);
			int index = 1;
			for (String value : queryAndValues.get(QueryConstants.columnValues)) {
				ps.setString(index, value);
				index++;
			}
			rs = ps.executeQuery();
			List<ProjectEntity> entities = getEntityList(rs, comp_type);

			if (entities != null && entities.size() >= 1)
				projectEntity = entities.get(0);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return projectEntity;
	}

	@Override
	public List<Component> findObjects(String comp_type, List<SearchCriterion> criterions, List<String> orderedList,
			String orderType) throws Exception {
		return null;
	}

	@Override
	public List<ProjectEntity> findObjects(String comp_type, List<SearchCriterion> criterions) throws Exception {

		List<ProjectEntity> entities = null;
		String tableName = ZDPDaoUtility.getTableName(comp_type);
		WhereClauseQueryFormatioin queryFormatioin = new WhereClauseQueryFormatioin();
		Map<String, List<String>> queryAndValues = queryFormatioin.getWhereClause(criterions);
		String query = "select *from " + tableName + " " + queryAndValues.get(QueryConstants.columnQuery).get(0) + " ";
		System.out.println("query:" + query);
		System.out.println("values :" + queryAndValues.get(QueryConstants.columnValues));
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(query);
			int index = 1;
			for (String value : queryAndValues.get(QueryConstants.columnValues)) {
				ps.setString(index, value);
				index++;
			}
			rs = ps.executeQuery();
			entities = getEntityList(rs, comp_type);

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return entities;
	}

	private List<ProjectEntity> getEntityList(ResultSet rs, String comp_type) throws SQLException {

		List<ProjectEntity> entities = new ArrayList<>();

		while (rs.next()) {
			ProjectEntity projectEntity = new ProjectEntity();
			switch (comp_type) {

			case "entity":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setName(rs.getString("name"));
				projectEntity.setJsonblob(rs.getString("json_data"));
				projectEntity.setCreated(rs.getTimestamp("created"));
				projectEntity.setCreatedBy(rs.getString("created_by"));
				projectEntity.setComponent_type(rs.getString("type"));
				break;
			case "project":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setName(rs.getString("name"));
				projectEntity.setJsonblob(rs.getString("design"));
				projectEntity.setVersion(String.valueOf(rs.getInt("version")));
				projectEntity.setCreated(rs.getTimestamp("created"));
				projectEntity.setCreatedBy(rs.getString("created_by"));
				projectEntity.setWorkspace_name(rs.getString("workspace_name"));

				// entity=new Entity(rs.getInt("id"), rs.getString("name"),
				// rs.getString("design"), rs.getInt("version"),
				// rs.getTimestamp("created_at"), rs.getString("user"));
				break;
			case "project_history":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				// id | project_id | version | oozie_id | run_mode | start_time
				// | end_time | createdBy | status | run_details
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setProject_id(rs.getLong("project_id"));
				projectEntity.setVersion(String.valueOf(rs.getInt("version")));
				projectEntity.setOozie_id(rs.getString("oozie_id"));
				projectEntity.setStart_time(rs.getTimestamp("start_time"));
				projectEntity.setEnd_time(rs.getTimestamp("end_time"));
				projectEntity.setStatus(rs.getString("status"));

				// entity=new Entity(rs.getInt("id"), rs.getString("name"),
				// rs.getString("design"), rs.getInt("version"),
				// rs.getTimestamp("created_at"), rs.getString("user"));
				break;
			case "module_history":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				// id | project_id | version | oozie_id | run_mode | start_time
				// | end_time | createdBy | status | run_details
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setProject_id(rs.getLong("module_id"));
				projectEntity.setVersion(String.valueOf(rs.getInt("version")));
				projectEntity.setOozie_id(rs.getString("oozie_id"));
				projectEntity.setStart_time(rs.getTimestamp("start_time"));
				projectEntity.setEnd_time(rs.getTimestamp("end_time"));
				projectEntity.setStatus(rs.getString("status"));
				projectEntity.setDetails(rs.getString("details"));

				// entity=new Entity(rs.getInt("id"), rs.getString("name"),
				// rs.getString("design"), rs.getInt("version"),
				// rs.getTimestamp("created_at"), rs.getString("user"));
				break;

			case "module":
				// component= new ZDPModule(rs.getInt("id"),
				// rs.getString("name"), rs.getString("prop"),
				// rs.getInt("version"),rs.getLong("proj_id"),
				// rs.getTimestamp("created_at"), rs.getString("user_id"));

				projectEntity.setId(rs.getLong("id"));
				projectEntity.setName(rs.getString("component_type"));
				projectEntity.setJsonblob(rs.getString("properties"));
				projectEntity.setVersion(String.valueOf(rs.getInt("version")));
				projectEntity.setProject_id(rs.getLong("project_id"));
				projectEntity.setCreated(rs.getTimestamp("created"));
				projectEntity.setCreatedBy(rs.getString("created_by"));
				break;

			case "workspace":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setName(rs.getString("name"));
				projectEntity.setCreated(rs.getTimestamp("created"));
				projectEntity.setCreatedBy(rs.getString("created_by"));
				break;

			case "ingestion_run_info":
				// component=new ZDPProject(rs.getInt("id"),
				// rs.getString("name"), rs.getString("design"),
				// rs.getInt("version"), rs.getTimestamp("created_at"),
				// rs.getString("user"));
				projectEntity.setId(rs.getLong("id"));
				projectEntity.setFilename(rs.getString("filename"));
				projectEntity.setSchemaname(rs.getString("schemaname"));
				projectEntity.setMd5(rs.getString("md5"));
				projectEntity.setCreated(rs.getTimestamp("created"));
				projectEntity.setCreatedBy(rs.getString("created_by"));
				break;
			}
			entities.add(projectEntity);
		}

		return entities;

	}

	@Override
	public List<ModuleSchema> getColumnNames(String comp_type, String id, String version) throws Exception {
		List<SearchCriterion> criterions = new ArrayList<>();
		SearchCriterion c1 = new SearchCriterion("id", id, SearchCriteriaEnum.EQUALS);
		SearchCriterion c2 = new SearchCriterion("version", version, SearchCriteriaEnum.EQUALS);
		criterions.add(c1);
		criterions.add(c2);
		ProjectEntity projectEntity = findExactObject(comp_type, criterions);
		String json = projectEntity.getJsonblob();
		List<ModuleSchema> columnList = null;
		if (json != null) {
			columnList = new ArrayList<>();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(json);
			JsonNode colAttrs = rootNode.path("params");
			JsonNode schema = colAttrs.path("columnList");
			for (JsonNode jsonNode : schema) {
				ModuleSchema each = new ModuleSchema(jsonNode.path("name").asText(),
						jsonNode.path("dataType").asText());
				columnList.add(each);
			}
		}
		return columnList;

	}

	@Override
	public ProjectEntity findLatestVersionObject(ProjectEntity projectEntity) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(QueryConstants.LATEST_MODULE);
			ps.setLong(1, projectEntity.getProject_id());
			ps.setString(2, projectEntity.getName());
			rs = ps.executeQuery();
			List<ProjectEntity> entities = getEntityList(rs, projectEntity.getSchemaType());
			if (entities != null && entities.size() > 0) {
				return entities.get(0);
			}
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return null;
	}

	@Override
	public List<ProjectEntity> findLatestVersionProjects(String schemaType) throws Exception {
		List<ProjectEntity> entities = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(QueryConstants.LATEST_PROJECT);
			rs = ps.executeQuery();
			entities = getEntityList(rs, schemaType);
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return entities;
	}

	@Override
	public List<Map<String, String>> getWFJobDeatails(String oozieId) throws Exception {

		List<Map<String, String>> jobList = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		Connection connection = null;
		try {

			String sQuery = "select id,app_name,start_time,end_time,status from WF_JOBS where id = ?";
			connection = ConnectionUtility.getOozieDbConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, oozieId);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Map<String, String> resultMap = new HashMap<>();
				resultMap.put("id", rs.getString("id"));
				resultMap.put("app_name", rs.getString("app_name"));
				resultMap.put("start_time", rs.getString("start_time"));
				resultMap.put("end_time", rs.getString("end_time"));
				resultMap.put("status", rs.getString("status"));
				jobList.add(resultMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return jobList;
	}

	@Override
	public List<Map<String, String>> getWFActionsDeatails(String oozieId) throws Exception {

		List<Map<String, String>> jobList = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		Connection connection = null;
		try {
			String sQuery = "select id, start_time, end_time, status from WF_ACTIONS where wf_id = ?";
			connection = ConnectionUtility.getOozieDbConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, oozieId);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Map<String, String> resultMap = new HashMap<>();
				resultMap.put("id", rs.getString("id"));
				resultMap.put("start_time", rs.getString("start_time"));
				resultMap.put("end_time", rs.getString("end_time"));
				resultMap.put("status", rs.getString("status"));
				jobList.add(resultMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return jobList;
	}

	@Override
	public List<ModuleSchema> getColumnAndDatatype(String datasetId) {
		// TODO Auto-generated method stub
		List<ModuleSchema> columns = new ArrayList<ModuleSchema>();

		try {
			ProjectEntity dataschema = getEntityDetails(datasetId);
			ObjectMapper mapper = new ObjectMapper();
			if (dataschema != null && dataschema.getId() != 0) {
				JsonNode rootNode = mapper.readTree(dataschema.getJsonblob());
				JsonNode colAttrs = rootNode.path("dataAttribute");

				for (JsonNode jsonNode : colAttrs) {
					columns.add(new ModuleSchema(jsonNode.path("Name").textValue(),
							jsonNode.path("dataType").textValue()));
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return columns;
	}

	// gets schema entity for dataset id
	public ProjectEntity getEntityDetails(String datasetId) throws SQLException {

		ProjectEntity dataset = getDatasetDetails(datasetId);
		ProjectEntity projectEntity = new ProjectEntity();
		if (dataset != null && dataset.getId() != 0) {
			String schemaName = "";
			try {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(dataset.getJsonblob());
				if(rootNode.get("Schema") !=null)
				    schemaName = rootNode.get("Schema").textValue();
				else if(rootNode.get("name")!=null)
					schemaName = rootNode.get("name").textValue();
				
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			ResultSet rs = null;
			try {
				String sQuery = "select  *from entity where name =?";
				connection = ConnectionUtility.getConnection();
				preparedStatement = connection.prepareStatement(sQuery);
				preparedStatement.setString(1, schemaName);
				rs = preparedStatement.executeQuery();
				while (rs.next()) {
					projectEntity.setId(rs.getInt("id"));
					projectEntity.setName(rs.getString("name"));
					projectEntity.setJsonblob(rs.getString("json_data"));

				}
			} catch (SQLException e) {
			} finally {
				ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
			}
		}
		return projectEntity;
	}

	private static ProjectEntity getDatasetDetails(String datasetId) throws SQLException {

		ProjectEntity projectEntity = new ProjectEntity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = "select  *from entity where id =?";
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, datasetId);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				projectEntity.setId(rs.getInt("id"));
				projectEntity.setName(rs.getString("name"));
				projectEntity.setJsonblob(rs.getString("json_data"));
			}
		} catch (SQLException e) {
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
		}
		return projectEntity;

	}

	/*
	 * update the column list for hive transformation
	 */
	private void updateJsonForColumnList(ProjectEntity projectEntity) {
		// only for test
		// String
		// json="{\"params\":{\"columnList\":{},\"hiveSql\":\"select t1.*,t2.*
		// from test_1 t1,test_2
		// t2\",\"dataset\":[{\"id\":\"12729_0\",\"tableName\":\"test_1\"},{\"id\":\"12701_0\",\"tableName\":\"test_2\"}]}}";
		// entity.setJsonblob(json);

		// end
		// ObjectMapper mapper = new ObjectMapper();
		// JsonNode rootNode;

		// store table name
		List<String> tempTable = new ArrayList<>();
		try {
			// rootNode = mapper.readTree(entity.getJsonblob());
			// JsonNode tempColAttrs=rootNode.get("params");
			// JsonNode colList=tempColAttrs.get("cloumnList");
			// String datasetList = tempColAttrs.get("dataset").getTextValue();
			// reading dataset
			List<HiveSchema> schemaList = new ArrayList<>();
			ObjectMapper hiveJsonObject = new ObjectMapper();
			JsonNode hiveRootNode = hiveJsonObject.readTree(projectEntity.getJsonblob());
			JsonNode tempColAttrs = hiveRootNode.get("params");
			JsonNode colList = tempColAttrs.get("dataset");
			Iterator<JsonNode> colListItr = colList.iterator();
			while (colListItr.hasNext()) {
				JsonNode temp = colListItr.next();
				schemaList.add(new HiveSchema(temp.get("id").textValue(), temp.get("tableName").textValue()));
			}
			// end
			String sqlQuery = tempColAttrs.get("hiveSql").textValue();

			if (schemaList != null && !(schemaList.isEmpty())) {
				// String[] datasetArr=sche.split(",");
				for (HiveSchema hiveSchema : schemaList) {
					String tableName = hiveSchema.getTableName();
					tempTable.add(tableName);
					String[] idVersion = hiveSchema.getIdAndVersion().split("_");
					List<SearchCriterion> criterions = new ArrayList<>();
					SearchCriterion c1 = new SearchCriterion("id", idVersion[0], SearchCriteriaEnum.EQUALS);
					SearchCriterion c2 = new SearchCriterion("version", idVersion[1], SearchCriteriaEnum.EQUALS);
					criterions.add(c1);
					criterions.add(c2);
					// read from module table
					ProjectEntity temp = findExactObject(ZDPDaoConstant.ZDP_MODULE_TABLE, criterions);
					StringBuilder columns = new StringBuilder();
					if (temp != null) {
						ObjectMapper tempMapper = new ObjectMapper();
						JsonNode tempRootNode = tempMapper.readTree(temp.getJsonblob());
						JsonNode colAttrs = tempRootNode.get("params");
						JsonNode schema = colAttrs.path("columnList");

						int count = 1;
						for (JsonNode jsonNode : schema) {
							String hiveToSQLDatatype = ZDPDaoUtility
									.getSQLDatatypeFromHive(jsonNode.path("dataType").textValue());
							columns.append(jsonNode.path("name").textValue() + " " + hiveToSQLDatatype);
							if (count < schema.size()) {
								columns.append(",");
							}
							count++;
						}
					} else {
						// read from entity table
						List<ModuleSchema> colNameAndDatatypeList = getColumnAndDatatype(idVersion[0]);
						int count = 1;
						for (ModuleSchema schema : colNameAndDatatypeList) {
							String hiveToSQLDatatype = ZDPDaoUtility.getSQLDatatypeFromHive(schema.getDataType());
							columns.append(schema.getName() + " " + hiveToSQLDatatype);
							if (count < colNameAndDatatypeList.size()) {
								columns.append(",");
							}
							count++;
						}
					}

					createTable(columns.toString(), tableName);
				}
				Map<String, String> columnAndDatatype = validateQuery(sqlQuery, "");
				// cleanTemporaryTable(tempTable);
				JSONArray jsonArr = ZDPDaoUtility.getJSONArray(columnAndDatatype);
			//	ObjectMapper latest = new ObjectMapper();
				JSONObject obj = new JSONObject(projectEntity.getJsonblob());
				//JsonNode entityLatest = latest.readTree(entity.getJsonblob());
				JSONObject columnList = obj.getJSONObject("params");
				columnList.put("columnList", jsonArr);
				projectEntity.setJsonblob(obj.toString());
				System.out.println(projectEntity.getJsonblob());

				// validate the query for given table

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// added finally for clean table which created temporary
		// to check schema for hive
		finally {
			try {
				cleanTemporaryTable(tempTable);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private boolean createTable(String columnName, String tableName) throws SQLException {

		boolean isCreated = false;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			// change the string to varchar(10)
			String sqlQuery = "create table " + tableName + " (" + columnName + ")";
			ps = conn.prepareStatement(sqlQuery);
			ps.execute();
			isCreated = true;
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isCreated;
	}

	private Map<String, String> validateQuery(String query, String tableName) {

		query = query.toLowerCase();
		/*
		 * int indexOfSelect=query.indexOf("select"); int indexOfFrom
		 * =query.lastIndexOf("from");
		 * 
		 * String[] arr=query.split(" "); StringBuilder columns= new
		 * StringBuilder(); for(String str:arr){
		 * 
		 * if(str.equalsIgnoreCase("from") || str.equalsIgnoreCase("*from") ||
		 * str.equalsIgnoreCase("* from")) { break; }
		 * if(!(str.equalsIgnoreCase("select") || str.equalsIgnoreCase("*"))) {
		 * columns.append(str); } }
		 */
		PreparedStatement ps = null;
		PreparedStatement ps1 = null;
		ResultSet rs = null;
		Map<String, String> columnAndDatatype = new LinkedHashMap<>();
		Connection conn = null;
		try {
			/*
			 * try{ if(columns.length()==0){ columns.append("*"); }
			 */
			// String
			// sql="create table "+ZDPDaoConstant.TEMPORART_HIVE_TABLE+" as
			// select "+columns.toString()+" from "+tableName+" where 1=2";
			String sql = "create table " + ZDPDaoConstant.TEMPORART_HIVE_TABLE + " as  " + query;// +"
																									// where
																									// 1=2";

			conn = ConnectionUtility.getConnection();
			ps1 = conn.prepareStatement(sql);
			ps1.execute();
			ps = conn.prepareStatement("describe " + ZDPDaoConstant.TEMPORART_HIVE_TABLE);
			rs = ps.executeQuery();

			while (rs.next()) {
				String hiveDatatype = ZDPDaoUtility.getHiveFromSQLDatatype(rs.getString(2));
				columnAndDatatype.put(rs.getString(1), hiveDatatype);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ps1 != null) {
				try {
					ps1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return columnAndDatatype;
	}

	// clean temporary table
	private void cleanTemporaryTable(List<String> tables) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			// change the string to varchar(10)
			StringBuilder tname = new StringBuilder();
			for (String name : tables) {
				tname.append(name + ",");
			}
			tname.append(ZDPDaoConstant.TEMPORART_HIVE_TABLE);
			// updated query with if exists
			String sqlQuery = "drop table if exists " + tname;
			ps = conn.prepareStatement(sqlQuery);
			ps.execute();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}

	}

	@Override
	public List<ProjectRunHistory> getProjectRunHistoryInfo(String name, String id) throws SQLException {

		String query = "select id,project_id,version,oozie_id,start_time,end_time,status,"
				+ "TIMESTAMPDIFF(SECOND,start_time,end_time) as time_elapsed  from project_history"
				+ " where project_id=? order by start_time desc";
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<ProjectRunHistory> runHistory = new ArrayList<>();
		try {
			connection = ConnectionUtility.getConnection();
			ps = connection.prepareStatement(query);
			ps.setString(1, id);
			rs = ps.executeQuery();
			while (rs.next()) {
				ProjectRunHistory pr = new ProjectRunHistory();
				pr.setName(name);
				pr.setId(rs.getLong("id"));
				pr.setProjectId(rs.getString("project_id"));
				pr.setVersion(rs.getString("version"));
				pr.setStatus(rs.getString("status"));
				pr.setStarted(rs.getTimestamp("start_time"));
				pr.setFinished(rs.getTimestamp("end_time"));
				Long seconds = rs.getLong("time_elapsed");
				if (seconds != null) {
					pr.setTimeTaken(ZDPDaoUtility.getTimeInHHMMSS(seconds));
				}
				runHistory.add(pr);
			}
		} catch (Exception e) {
			System.out.println(e.toString());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, connection);
		}
		// TODO Auto-generated method stub
		return runHistory;
	}

	@Override
	public boolean addUserAction(Map<String, String> actionInfo) throws SQLException {
		logger.info("adduser information start:" + actionInfo);
		boolean isCreated = false;
		int count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			String sql = "insert into useraction(action_type,action_id,action_name,created_by,created) values(?,?,?,?,now())";
			ps = conn.prepareStatement(sql);
			ps.setObject(1, actionInfo.get("action_type"));
			ps.setObject(2, actionInfo.get("action_id"));
			ps.setObject(3, actionInfo.get("action_name"));
			ps.setObject(4, actionInfo.get("created_by"));
			count = ps.executeUpdate();
			if (count > 0) {
				isCreated = true;
			}
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isCreated;
	}

	@Override
	public String getUserForAction(String name) throws SQLException {
		// TODO Auto-generated method stub
		logger.info("find user for give ingestion or project run:" + name);
		String usrName = "";
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		String sql = "select *from useraction where action_name=?";
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, name);
			rs = ps.executeQuery();
			while (rs.next()) {
				usrName = rs.getString("action_name");
			}
		} catch (SQLException ex) {
			logger.error(ex.toString());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		logger.info("resulted username :" + usrName);
		return usrName;
	}

	@Override
	public boolean deleteUserAction(String userName) throws SQLException {
		// TODO Auto-generated method stub
		logger.info("user name:" + userName);
		boolean isDeleted = false;
		int count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			String sql = "delete from useraction where created_by=?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, userName);
			count = ps.executeUpdate();
			if (count > 0) {
				isDeleted = true;
			}
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isDeleted;
	}

	@Override
	public List<String> getUserListForGivenId(String id, String type) throws Exception {
		// TODO Auto-generated method stub

		List<String> groupIds = getUserGroupId(id, type);
		List<String> userList = new ArrayList<>();
		StringBuilder users = new StringBuilder();
		if (groupIds != null && groupIds.size() > 0) {
			int size = groupIds.size();
			int count = 1;
			for (String group : groupIds) {
				users.append("'" + group + "'");
				if (count < size) {
					users.append(",");
				}
				count++;
			}
			String sql = "select distinct user_id from user_group where group_id in(" + users.toString() + ")";
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				conn = ConnectionUtility.getConnection();
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				while (rs.next()) {
					userList.add(rs.getString("user_id"));
				}
			} catch (Exception e) {
				logger.error(e.toString());
			} finally {
				ConnectionUtility.releaseConnectionResources(rs, ps, conn);
			}
			logger.info("result of user list :" + userList);
		}
		return userList;
	}

	private List<String> getUserGroupId(String id, String type) throws Exception {

		logger.info("id :" + id + "  ,type:" + type);
		String sql = "select distinct group_id from module_permission where module_id=?";
		;
		if (ZDPDaoConstant.ZDP_INGESTION.equalsIgnoreCase(type)) {
			sql = "select distinct group_id from dataset_permission where module_id=?";
		} else if (ZDPDaoConstant.ZDP_PROJECT.equalsIgnoreCase(type)) {
			sql = "select distinct group_id from project_permission where project_id=?";
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<String> listOfGroupId = new ArrayList<>();
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setObject(1, id);
			rs = ps.executeQuery();
			while (rs.next()) {
				listOfGroupId.add(rs.getString("group_id"));
			}
		} catch (Exception e) {
			logger.error(e.toString());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		logger.info("result of getUserGroupId :" + listOfGroupId);
		return listOfGroupId;
	}

	@Override
	public void addActivitiesBatchForNewAPI(String entityName, String statusInfo, String component, String operation,
			List<String> users, String action_user) throws SQLException {

		/*
		 * // Add into activities_info table: String statusInfo = "";
		 * 
		 * switch (operation) { case ZDPDaoConstant.CREATE_ACTIVITY: statusInfo
		 * = "New " + component + " profile '" + entityName + "' created";
		 * break; case ZDPDaoConstant.UPDATE_ACTIVITY: statusInfo = component +
		 * " '" + entityName + "' edited"; break; case
		 * ZDPDaoConstant.EXECUTE_ACTIVITY: statusInfo = component + " '" +
		 * entityName + "' executed"; break; case
		 * ZDPDaoConstant.DELETE_ACTIVITY: statusInfo = "The " + component +
		 * " '" + entityName + "' deleted"; break; default: break; }
		 */
		Connection connection = null;
		PreparedStatement ps = null;
		String tablename = "";
		switch (component) {
		case ZDPDaoConstant.ZDP_INGESTION:
			tablename = "entity";
			break;
		case ZDPDaoConstant.ZDP_PROJECT:
			tablename = "project";
			break;
		}
		try {
			connection = ConnectionUtility.getConnection();
			String sql = "insert into activities(component_id,component_key_id,operation_key_id,status_messege,action_user_id) "
					+ "select distinct d.id,a.id,b.id,?,c.id from component_key a,operation_key b,user c," + tablename
					+ " d" + " where a.component_value=? and b.operation_value=? and c.name=? and d.name=?";
			ps = connection.prepareStatement(sql);
			for (int i=0;i< users.size();i++) {
				ps.setString(1, statusInfo);
				ps.setString(2, component.toUpperCase());
				ps.setString(3, operation.toUpperCase());
				ps.setString(4, action_user);
				ps.setString(5, entityName);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, connection);
		}
	}
	
	public void addBulkActivitiesBatchForNewAPI(String entityName, String statusInfo, String component, String operation,
			List<String> users, String action_user) throws SQLException {

		/*
		 * // Add into activities_info table: String statusInfo = "";
		 * 
		 * switch (operation) { case ZDPDaoConstant.CREATE_ACTIVITY: statusInfo
		 * = "New " + component + " profile '" + entityName + "' created";
		 * break; case ZDPDaoConstant.UPDATE_ACTIVITY: statusInfo = component +
		 * " '" + entityName + "' edited"; break; case
		 * ZDPDaoConstant.EXECUTE_ACTIVITY: statusInfo = component + " '" +
		 * entityName + "' executed"; break; case
		 * ZDPDaoConstant.DELETE_ACTIVITY: statusInfo = "The " + component +
		 * " '" + entityName + "' deleted"; break; default: break; }
		 */
		Connection connection = null;
		PreparedStatement ps = null;
		String tablename = "";
		switch (component) {
		case ZDPDaoConstant.ZDP_INGESTION:
			tablename = "bulk_entity";
			break;
		case ZDPDaoConstant.ZDP_PROJECT:
			tablename = "project";
			break;
		}
		try {
			connection = ConnectionUtility.getConnection();
			String sql = "insert into activities(component_id,component_key_id,operation_key_id,status_messege,action_user_id) "
					+ "select distinct d.id,a.id,b.id,?,c.id from component_key a,operation_key b,user c," + tablename
					+ " d" + " where a.component_value=? and b.operation_value=? and c.name=? and d.jobname=?";
			ps = connection.prepareStatement(sql);
			for (int i=0;i< users.size();i++) {
				ps.setString(1, statusInfo);
				ps.setString(2, component.toUpperCase());
				ps.setString(3, operation.toUpperCase());
				ps.setString(4, action_user);
				ps.setString(5, entityName);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, connection);
		}
	}
	

	@Override
	public void addComponentExecution(String entityName, String component, String userName) throws SQLException {
		Connection connection = null;
		PreparedStatement ps = null;
		String tablename = "entity";
		switch (component) {
		case ZDPDaoConstant.ZDP_INGESTION:
			tablename = "entity";
			break;
		case ZDPDaoConstant.STREAM_ACTIVITY:
			tablename = "streaming_entity";
			break;
		case ZDPDaoConstant.ZDP_PROJECT:
			tablename = "project";
			break;
		}
		try {
			connection = ConnectionUtility.getConnection();
			String sql = "insert into component_execution(component_id,component_type,user_id) select distinct a.id,b.id,c.id from "
					+ tablename + " a,component_key b,user c where a.name=? and b.component_value=? and c.name=?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, entityName);
			ps.setString(2, component.toUpperCase());
			ps.setString(3, userName);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, connection);
		}
	}

	@Override
	public void addComponentRunStatus(String entityName, String componentType, String runStatus, String jobId,
			String userName) throws SQLException {
		Connection connection = null;
		PreparedStatement ps = null;
		String tablename = "entity";
		switch (componentType) {
		case ZDPDaoConstant.ZDP_INGESTION:
			tablename = "entity";
			break;
		case ZDPDaoConstant.STREAM_ACTIVITY:
			tablename = "entity";

			break;
		case ZDPDaoConstant.ZDP_PROJECT:
			tablename = "project";
			break;
		}
		try {
			if (componentType.equalsIgnoreCase(ZDPDaoConstant.STREAM_ACTIVITY)
					&& (!runStatus.equalsIgnoreCase(ZDPDaoConstant.JOB_RUNNING))) {
				addComponentForStream(jobId, runStatus);
			} else {
				connection = ConnectionUtility.getConnection();
				String sql = "insert into comp_exce_status(comp_exce_id,job_type,job_id,job_status_id)"
						+ " select a.id,b.id,?,c.id from component_execution a,"
						+ "component_key b,job_status c  where a.id in(select id from component_execution a"
						+ " where a.component_id=(select distinct id from " + tablename + " where name=?)"
						+ " and a.user_id=(select id from user where name=?) and time_stamp in(select MAX(time_stamp) as "
						+ "latest_time from component_execution group by component_type,component_id,user_id)) and"
						+ " b.component_value=? and c.status=?;";
				ps = connection.prepareStatement(sql);
				ps.setString(1, jobId);
				ps.setString(2, entityName);
				ps.setString(3, userName);
				ps.setString(4, componentType.toUpperCase());
				ps.setString(5, runStatus);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, connection);
		}
	}

	private void addComponentForStream(String jobId, String runStatus) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet resultSet = null;
		String sql = "select * from comp_exce_status where job_id='" + jobId + "' and job_type=3;";
		connection = ConnectionUtility.getConnection();
		try {
			ps = connection.prepareStatement(sql);
			resultSet = ps.executeQuery();
			long comp_exce_id = 0;
			while (resultSet.next()) {
				comp_exce_id = resultSet.getLong("comp_exce_id");
			}
			ps.close();
			resultSet.close();
			sql = "insert into  comp_exce_status(comp_exce_id,job_type,job_id,job_status_id) select ?,3,?,id from job_status where status=?;";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, comp_exce_id);
			ps.setString(2, jobId);
			ps.setString(3, runStatus);
			ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet, ps, connection);
		}
	}

	@Override
	public String exportProject(ProjectEntity projectEntity, String exportPath) throws Exception {

		logger.info("export path:" + exportPath + " project name :" + projectEntity.getName() + " id:" + projectEntity.getId()
				+ " version:" + projectEntity.getVersion());
		Long id = projectEntity.getId();
		String version = projectEntity.getVersion();
		SearchCriterion criterion1 = new SearchCriterion("id", id.toString(), SearchCriteriaEnum.EQUALS);
		SearchCriterion criterion2 = new SearchCriterion("version", version.toString(), SearchCriteriaEnum.EQUALS);
		// QueryCriterion criterion2= new QueryCriterion("salary", "5000",
		// QueryCriteriaEnum.GREATER_THAN_OR_EQUAL,QueryConstants.QUERY_OR_TYPE);
		List<SearchCriterion> list = new ArrayList<>();
		list.add(criterion1);
		list.add(criterion2);
		ProjectEntity project = findExactObject(ZDPDaoConstant.ZDP_PROJECT, list);
		JSONObject jsonObject = new JSONObject(project.getJsonblob());
		Set<String> moduleIdsAndVersion = ZDPDaoUtility.getModuleIdsAndVersion(project.getJsonblob());
		Iterator<String> itr = moduleIdsAndVersion.iterator();
		StringBuilder idsAndVersion = new StringBuilder();
		while (itr.hasNext()) {
			String idAndVersion = itr.next();
			if (idAndVersion != null && !idAndVersion.isEmpty()) {
				String arr[] = idAndVersion.split("-");
				Long module_Id = Long.parseLong(arr[0]);
				// Integer module_version=Integer.parseInt(arr[1]);
				SearchCriterion ctt1 = new SearchCriterion("id", module_Id.toString(), SearchCriteriaEnum.EQUALS);
				List<SearchCriterion> ctList = new ArrayList<>();
				ctList.add(ctt1);
				ProjectEntity module = null;
				module = findExactObject(ZDPDaoConstant.ZDP_MODULE_TABLE, ctList);
				String type = "module";
				if (module == null || (module != null && module.getId() == 0)) {
					module = findExactObject(ZDPDaoConstant.ZDP_ENTITY_TABLE, ctList);
					type = "DataSet";
				}
				// List<String> idsAndVersion=new ArrayList<>();
				if ("DataSet".equalsIgnoreCase(type)) {
					// add all DataSchema,DataSet,DataIngestion,DataSource json
					// entry in project json
					String dataSetName = module.getName();
					int index = dataSetName.lastIndexOf("DataSet");
					dataSetName = dataSetName.substring(0, index - 1);
					SearchCriterion ct1 = new SearchCriterion("name", dataSetName, SearchCriteriaEnum.EQUALS,
							QueryConstants.QUERY_OR_TYPE);
					SearchCriterion ct2 = new SearchCriterion("name", dataSetName + "_DataSet",
							SearchCriteriaEnum.EQUALS, QueryConstants.QUERY_OR_TYPE);
					SearchCriterion ct3 = new SearchCriterion("name", dataSetName + "_Schedular",
							SearchCriteriaEnum.EQUALS, QueryConstants.QUERY_OR_TYPE);
					SearchCriterion ct4 = new SearchCriterion("name", dataSetName + "_Source",
							SearchCriteriaEnum.EQUALS, QueryConstants.QUERY_OR_TYPE);
					List<SearchCriterion> ctrList = new ArrayList<>();
					ctrList.add(ct1);
					ctrList.add(ct2);
					ctrList.add(ct3);
					ctrList.add(ct4);
					List<ProjectEntity> allProjectEntity = findObjects(ZDPDaoConstant.ZDP_ENTITY_TABLE, ctrList);
					for (ProjectEntity temp : allProjectEntity) {
						JSONObject jsonTempObject = new JSONObject();
						String tempId = String.valueOf(temp.getId());
						String tempversion = String.valueOf(temp.getVersion());
						String tempType = String.valueOf(temp.getComponent_type());
						String tempJson = String.valueOf(temp.getJsonblob());
						jsonTempObject.put("id", tempId);
						jsonTempObject.put("version", tempversion);
						jsonTempObject.put("type", tempType);
						jsonTempObject.put("json", tempJson);
						idsAndVersion.append(tempId + "-" + tempversion + ",");
						jsonObject.put(tempId + "-" + tempversion, jsonTempObject);

					}

					System.out.println(allProjectEntity);

				} else {
					// add tranaformation json in project json
					JSONObject jsonTempObject = new JSONObject();
					String tempId = String.valueOf(module.getId());
					String tempversion = String.valueOf(module.getVersion());
					String tempType = String.valueOf(module.getName());
					String tempJson = String.valueOf(module.getJsonblob());
					// jsonTempObject.put("id", tempId);
					// jsonTempObject.put("version", tempversion);
					jsonTempObject.put("type", tempType);
					jsonTempObject.put("json", tempJson);
					jsonObject.put(tempId + "-" + tempversion, jsonTempObject);
					idsAndVersion.append(tempId + "-" + tempversion + ",");
				}

			}
		}
		jsonObject.put("idsAndVersion", idsAndVersion.toString());
		System.out.println(jsonObject.toString());
		File file = new File(exportPath + "/" + "project_" + projectEntity.getId() + "_" + projectEntity.getVersion());
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write(jsonObject.toString());
		writer.close();
		System.out.println("file created");
		return "project export succeded";

	}

	@Override
	public String importProject(String filePath, String user) throws Exception {

		logger.info("import file location :" + filePath);
		String importMsg = "project import failed";
		String projectName = "";
		File file = new File(filePath);
		StringBuilder sb = new StringBuilder();
		String projectJson = "";
		Map<String, String> keyAndValues = new HashMap<>();
		try {
			BufferedReader bf = new BufferedReader(new FileReader(file));
			String str = "";
			while ((str = bf.readLine()) != null) {
				sb.append(str);
			}
			bf.close();
		} catch (IOException e) {
			logger.info(e.toString());
			throw new IOException(e);
		}

		// write project
		if (sb != null && sb.length() != 0) {
			projectJson = sb.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(sb.toString());
			String pName = node.get("name").asText();
			String workspace = "Default";
			if (node.get("workspace") != null)
				workspace = node.get("workspace").asText();
			ProjectEntity prProjectEntity = new ProjectEntity();
			prProjectEntity.setName(pName);
			projectName = pName;
			prProjectEntity.setCreatedBy(user);
			prProjectEntity.setComponent_type(ZDPDaoConstant.ZDP_PROJECT);
			prProjectEntity.setSchemaType(ZDPDaoConstant.ZDP_PROJECT);
			prProjectEntity.setWorkspace_name(workspace);
			prProjectEntity.setJsonblob(sb.toString());
			prProjectEntity = addEntity(prProjectEntity);
			// if (prEntity != null) {
			// ZDPUserAccess userAccess = new ZDPUserAccessImpl();
			// userAccess.addEntryInResPermissionForDefaultUGroup(
			// UserManagementConstant.ResourceType.PROJECT,
			// prEntity.getId(), user);
			// }
			String moduleIdsAndVersion = node.get("idsAndVersion").asText();
			String[] moduleIdsAndVersionArr = moduleIdsAndVersion.split(",", -1);
			for (String idVersion : moduleIdsAndVersionArr) {

				if (idVersion != null && !idVersion.isEmpty()) {
					JsonNode tempjson = node.get(idVersion);
					String type = tempjson.get("type").asText();
					if (tempjson != null) {
						// find the dataset if doesn't exist create it
						List<SearchCriterion> crList = new ArrayList<>();
						String arr[] = idVersion.split("-");
						SearchCriterion cr1 = new SearchCriterion("id", arr[0], SearchCriteriaEnum.EQUALS);
						SearchCriterion cr2 = new SearchCriterion("version", arr[1], SearchCriteriaEnum.EQUALS);
						crList.add(cr1);
						crList.add(cr2);
						String objectType = ZDPDaoUtility.getType(type);
						if (ZDPDaoConstant.ZDP_ENTITY_TABLE.equalsIgnoreCase(objectType)) {
							// remove the version for entity table
							crList.remove(1);
							ProjectEntity tempProjectEntity = findExactObject(objectType, crList);
							// JsonNode ttNode = tempjson.get("json");
							String strJson = "";
							JsonNode ttNode = tempjson.get("json");
							strJson = ttNode.textValue();
							JsonNode nm = mapper.readTree(strJson);
							String name = nm.get("name").asText();
							// String strJsonDB=tempEntity.getJsonblob();
							// name=nm.get("name").asText();
							if (tempProjectEntity != null && tempProjectEntity.getId() > 0) {
								/*
								 * JsonNode ttNode = tempjson.get("json");
								 * strJson=ttNode.getTextValue(); JsonNode
								 * nm=mapper.readTree(strJson);
								 */
								name = nm.get("name").asText();
								String tempName = tempProjectEntity.getName();
								// if(tempName.equalsIgnoreCase(name) &&
								// !strJson.equalsIgnoreCase(strJsonDB)){
								if (tempName.equalsIgnoreCase(name)) {
									System.out.println("already exist .=>" + name);
								} else {
									// create datset
									ProjectEntity projectEntity = new ProjectEntity();
									projectEntity.setName(name);
									projectEntity.setComponent_type(type);
									projectEntity.setJsonblob(strJson);
									projectEntity.setCreatedBy(user);
									projectEntity = addSchema(projectEntity);
									if (projectEntity != null && projectEntity.getId() > 0) {
										String newIDVersion = projectEntity.getId() + "-" + projectEntity.getVersion();
										projectJson = projectJson.replaceAll(idVersion, newIDVersion);
										// String graph update
										ObjectMapper mapper11 = new ObjectMapper();
										String prtemp = projectJson;
										JsonNode node11 = mapper11.readTree(prtemp);
										JsonNode nn11 = node11.get("ExecutionGraph");
										JsonNode n11 = nn11.get("nodes");
										Iterator<JsonNode> itr = n11.elements();
										String[] oldIdVersion = idVersion.split("-");
										while (itr.hasNext()) {
											JsonNode n = itr.next();
											// JsonNode nm=n.readTree(n);
											String blockId = n.get("blockId").asText();
											String nodeType = n.get("nodetype").asText();
											if ("Dataset".equalsIgnoreCase(nodeType)) {
												String newBlockId = blockId.replaceAll(oldIdVersion[0],
														projectEntity.getId() + "");
												newBlockId = newBlockId.replaceAll("version" + oldIdVersion[1],
														"version" + projectEntity.getVersion());
												projectJson = projectJson.replaceAll(blockId, newBlockId);
											}
										}
										// end graph update
									}
								}

							} else {
								// create dataset
								ProjectEntity projectEntity = new ProjectEntity();
								projectEntity.setName(name);
								projectEntity.setComponent_type(type);
								projectEntity.setJsonblob(strJson);
								projectEntity.setCreatedBy(user);
								projectEntity = addSchema(projectEntity);
								if (projectEntity != null && projectEntity.getId() > 0) {
									String newIDVersion = projectEntity.getId() + "-" + projectEntity.getVersion();
									projectJson = projectJson.replaceAll(idVersion, newIDVersion);
									// String graph update
									ObjectMapper mapper11 = new ObjectMapper();
									String prtemp = projectJson;
									JsonNode node11 = mapper11.readTree(prtemp);
									JsonNode nn11 = node11.get("ExecutionGraph");
									JsonNode n11 = nn11.get("nodes");
									Iterator<JsonNode> itr = n11.elements();
									String[] oldIdVersion = idVersion.split("-");
									while (itr.hasNext()) {
										JsonNode n = itr.next();
										// JsonNode nm=n.readTree(n);
										String blockId = n.get("blockId").asText();
										String nodeType = n.get("nodetype").asText();
										if ("Dataset".equalsIgnoreCase(nodeType)) {
											String newBlockId = blockId.replaceAll(oldIdVersion[0],
													projectEntity.getId() + "");
											newBlockId = newBlockId.replaceAll("version" + oldIdVersion[1],
													"version" + projectEntity.getVersion());
											projectJson = projectJson.replaceAll(blockId, newBlockId);
										}
									}
									// end graph update
								}

							}
						} else {
							// create transformation
							ProjectEntity projectEntity = new ProjectEntity();
							String strJson = tempjson.get("json").asText();
							projectEntity.setSchemaType(ZDPDaoConstant.ZDP_MODULE_TABLE);
							projectEntity.setComponent_type(type);
							projectEntity.setName(type);
							projectEntity.setJsonblob(strJson);
							projectEntity.setCreatedBy(user);
							projectEntity.setProject_id(prProjectEntity.getId());
							projectEntity = addEntity(projectEntity);
							// if (entity != null) {
							// ZDPUserAccess userAccess = new
							// ZDPUserAccessImpl();
							// userAccess.addEntryInResPermissionForDefaultUGroup(
							// UserManagementConstant.ResourceType.DATASET,
							// entity.getId(), user);
							// }
							String newIDVersion = projectEntity.getId() + "-" + projectEntity.getVersion();
							projectJson = projectJson.replaceAll(idVersion, newIDVersion);
							// String graph update
							ObjectMapper mapper11 = new ObjectMapper();
							String prtemp = projectJson;
							JsonNode node11 = mapper11.readTree(prtemp);
							JsonNode nn11 = node11.get("ExecutionGraph");
							JsonNode n11 = nn11.get("nodes");
							Iterator<JsonNode> itr = n11.elements();
							String[] oldIdVersion = idVersion.split("-");
							while (itr.hasNext()) {
								JsonNode n = itr.next();
								// JsonNode nm=n.readTree(n);
								String blockId = n.get("blockId").asText();
								String nodeType = n.get("nodetype").asText();
								if (!("Dataset".equalsIgnoreCase(nodeType))) {
									String newBlockId = blockId.replaceAll(oldIdVersion[0], projectEntity.getId() + "");
									newBlockId = newBlockId.replaceAll("version" + oldIdVersion[1],
											"version" + projectEntity.getVersion());
									projectJson = projectJson.replaceAll(blockId, newBlockId);
								}
							}
							// end graph update

						}

					}

					System.out.println(tempjson);
				}
			}
			// Iterator<String> itr=moduleIdsAndVersion.iterator();
			// while(itr.hasNext()) {
			// String key=itr.next();
			// JsonNode json=node.get(key);
			// keyAndValues.put(key, node.get(key).asText());
			// }
			System.out.println(keyAndValues);
			// update project json
			ObjectMapper mm = new ObjectMapper();
			JsonNode jsonNode = mm.readTree(projectJson);
			String sdd = jsonNode.toString();
			// sdd=sdd.replace("\\", "\\\\");
			Map<String, String> projectMap = new HashMap<>();
			projectMap.put("design", sdd);
			List<SearchCriterion> updateList = new ArrayList<>();
			SearchCriterion cr11 = new SearchCriterion("id", String.valueOf(prProjectEntity.getId()),
					SearchCriteriaEnum.EQUALS);
			SearchCriterion cr12 = new SearchCriterion("version", String.valueOf(prProjectEntity.getVersion()),
					SearchCriteriaEnum.EQUALS);
			updateList.add(cr11);
			updateList.add(cr12);
			// boolean isUpdate=updateObject(ZDPDaoConstant.ZDP_PROJECT,
			// projectMap, updateList);
			boolean isUpdate = updateJson(sdd, String.valueOf(prProjectEntity.getId()), String.valueOf(prProjectEntity.getVersion()));
			importMsg = "'" + projectName + "'" + " project imported successfully";
			System.out.println("project json update:" + isUpdate);

		}
		return importMsg;

	}

	private ProjectEntity addSchema(ProjectEntity projectEntity) {

		logger.info("adduser information start:" + projectEntity);
		ResultSet rs = null;
		long id = 0;
		Connection conn = null;
		String sql = "insert into entity(NAME,TYPE,JSON_DATA,CREATED_BY,CREATED,IS_ACTIVE) values(?,?,?,?,now(),?)";
		PreparedStatement ps = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			ps.setObject(1, projectEntity.getName());
			ps.setObject(2, projectEntity.getComponent_type());
			ps.setObject(3, projectEntity.getJsonblob());
			ps.setObject(4, projectEntity.getCreatedBy());
			ps.setObject(5, "1");
			ps.execute();
			rs = ps.getGeneratedKeys();
			while (rs.next()) {
				id = rs.getLong(1);
				projectEntity.setId(id);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return projectEntity;
	}

	public boolean updateJson(String json, String id, String version) {

		Connection conn = null;
		PreparedStatement ps = null;
		String sql = "update project set design=? where id =? and version=?";

		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			Reader rd = new StringReader(json);
			ps.setClob(1, rd);
			ps.setObject(2, id);
			ps.setObject(3, version);
			ps.executeUpdate();
			System.out.println("updated..");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return true;
	}

	@Override
	public List<String> getUsedProjectList(String dataSetName) throws Exception {
		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		List<String> projectList = new ArrayList<>();
		List<ProjectEntity> projectEntityList = dao.findLatestVersionProjects("project");
		for (ProjectEntity projectEntity : projectEntityList) {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode projectNode = mapper.readTree(projectEntity.getJsonblob());
			if (projectNode.get("ExecutionGraph").get("nodes") != null) {
				Iterator<JsonNode> nodesItr = projectNode.get("ExecutionGraph").get("nodes").iterator();
				while (nodesItr.hasNext()) {
					JsonNode node = nodesItr.next();
					if (node.get("nodetype").textValue().equalsIgnoreCase("Dataset")) {
						String dataSet = node.get("name").textValue();
						if (dataSet.equalsIgnoreCase(dataSetName))
							projectList.add(projectNode.get("name").textValue());
					}
				}
			}
		}

		return projectList;
	}

	@Override
	public List<ZDPModuleHistory> getModuleHistory(String project_run_Id, String moduleId, String version)
			throws Exception {

		List<ZDPModuleHistory> mhList = new ArrayList<>();
		String sql = "select  id,module_id,version,oozie_id,project_run_id,output_blob,start_time,end_time, created_by,status,details from module_history "
				+ "where project_run_id=? and module_id=? and version=?)";
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, project_run_Id);
			ps.setString(2, moduleId);
			ps.setString(3, version);
			rs = ps.executeQuery();
			while (rs.next()) {
				ZDPModuleHistory moduleHistory = new ZDPModuleHistory();
				moduleHistory.setId(rs.getLong("id"));
				moduleHistory.setModule_id(rs.getInt("module_id"));
				moduleHistory.setVersion(rs.getInt("version"));
				moduleHistory.setOozie_id(rs.getString("oozie_id"));
				moduleHistory.setProject_run_id(rs.getLong("project_run_id"));
				moduleHistory.setOutput_blob(rs.getString("output_blob"));
				moduleHistory.setStart_time(rs.getTimestamp("start_time"));
				moduleHistory.setEnd_time(rs.getTimestamp("end_time"));
				moduleHistory.setCreated_by(rs.getString("created_by"));
				moduleHistory.setStatus(rs.getString("status"));
				moduleHistory.setDetails(rs.getString("details"));
				mhList.add(moduleHistory);
			}
		} catch (Exception ex) {
			throw new Exception(ex);
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return mhList;
	}

	@Override
	public boolean createRunLogDetail(String name, String type, String status, String logFileLocation,
			String created_by) throws Exception {
		// TODO Auto-generated method stub
		// insert into
		// runlogdetails(name,type,status,createdBy,logfilelocation)
		boolean isCreated = false;
		logger.info("enter into createRunLogDetails API");
		String query = "insert into runlogdetails(name,type,status,logfilelocation,created_by) values(?,?,?,?,?)";
		Connection conn = ConnectionUtility.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, name);
			ps.setString(2, type);
			ps.setString(3, status);
			ps.setString(4, logFileLocation);
			ps.setString(5, created_by);
			ps.execute();
			isCreated = true;

		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(ps, conn);
		}
		return isCreated;
	}

	@Override
	public ZDPRunLogDetails getLatestRunLogDetail(String name, String userName) throws Exception {
		// TODO Auto-generated method stub
		logger.info("enter into getLatestRunLogDetail api");
		String query = "select *from runlogdetails where name=?  order by created desc limit 1";
		PreparedStatement ps = null;
		ZDPRunLogDetails logDetails = null;
		ResultSet rs = null;
		Connection conn = ConnectionUtility.getConnection();
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, name);
			// ps.setString(2, userName);
			rs = ps.executeQuery();
			logDetails = new ZDPRunLogDetails();
			while (rs.next()) {
				logDetails.setId(rs.getString("id"));
				logDetails.setName(rs.getString("name"));
				logDetails.setType(rs.getString("type"));
				logDetails.setStatus(rs.getString("status"));
				logDetails.setCreated_by(rs.getString("created_by"));
				logDetails.setLogfilelocation(rs.getString("logfilelocation"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return logDetails;
	}

	@Override
	public Boolean isProjectExist(String projectName, String userName) throws SQLException {
		// TODO Auto-generated method stub
		logger.info("enter into isProjectExist api");
		logger.info("projectname :" + projectName + "\nusername:" + userName);
		String query = "select name from project where lower(name)=?  and created_by =?  limit 1";
		logger.debug("isProjectExist query :" + query);
		PreparedStatement ps = null;
		ResultSet rs = null;
		Boolean isProjectExist = false;
		Connection conn = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(query);
			ps.setString(1, projectName);
			ps.setString(2, userName);
			rs = ps.executeQuery();
			if (rs.next()) {
				isProjectExist = true;
			}
			logger.info("successfully executed isProjectExist api with isProjectExist:" + isProjectExist);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.toString());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return isProjectExist;
	}

	@Override
	public Map<Long, String> getProjectSchema(Long projectId) throws SQLException {

		Map<Long, String> map = new HashMap<>();

		String sql = "select id ,properties  from module  where project_id=? and component_type NOT IN('internal dataset') ORDER BY created DESC Limit 1";
		/*
		 * String sql =
		 * "select  id,module_id,version,oozie_id,project_run_id,output_blob,start_time,end_time, createdBy,status,details from module_history "
		 * + "where project_run_id=? and module_id=? and version=?)";
		 */
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setLong(1, projectId);
			rs = ps.executeQuery();
			while (rs.next()) {
				map.put(rs.getLong("id"), rs.getString("properties"));
			}
		} catch (Exception ex) {
			try {
				throw new Exception(ex);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}

		return map;
	}

	@Override
	public Map<String, String> getProjectOuputDetails(Long moduleId) throws SQLException {


		String sql = "SELECT details FROM module_history where module_id= ? ORDER BY end_time DESC Limit 1";

		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;

		String details = null;
		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setLong(1, moduleId);
			rs = ps.executeQuery();
			while (rs.next()) {
				details = rs.getString("details");
			}
		} catch (Exception ex) {
			try {
				throw new Exception(ex);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return outPutDetails(details);

	}

	private Map<String, String> outPutDetails(String s) {

		Map<String, String> map = new HashMap<>();

		if (s.contains("Hive table")) {
			// String outPut = s.substring(s.indexOf("table")+5);
			map.put("Hive table", s.substring(s.indexOf("table") + 5));
		} else if (s.contains("HDFS path")) {
			map.put("HDFS path", s.substring(s.indexOf("path") + 5));

		}

		return map;
	}

	/**
	 * @author Shishir.Sarkar
	 * @param scheduler
	 * @return generatedId
	 */
	@Override
	final public void persistScedulerDetail(final ZDPScheduler scheduler) {
		// update zeas.scheduler set
		// name='real',startTime='07/10/2016',endTime='07/10/2016',frequency='2',repeats='daily'
		// where project_id=1807;
		Connection conn = null;
		String currentFormat = "dd/MM/yyyy HH:mm";
		String targetFormat  = "yyyy-MM-dd'T'HH:mm";
		PreparedStatement preparedStatement = null;

		try {
			conn = ConnectionUtility.getConnection();
			ZDPScheduler responseScheduler = getScheduler(scheduler.getProjectId());
			/**
			 * Get datasetName based on projectId and take Max version in case its blank
			 * persist datasetName in scheduler.
			 */
			//List<String> projectList = getDataSetFromProject(scheduler.getProjectId(), "");
			Long projectId = (long) scheduler.getProjectId();
			List<String> projectList = getDataSetFromProject(projectId, 0);
            String datasetName = projectList.get(0);
            
            scheduler.setDataset(datasetName);
            
			if (responseScheduler.getId() > 0) {
				
				scheduler.setStatus("New");
				
				final String query = "update scheduler set startTime=?,endTime=?,frequency=?,repeats=? ,type=?,tranformType=?,dataset=?,status=? where project_id=?;";
				preparedStatement = conn.prepareStatement(query);

				preparedStatement.setString(1, ZDPDaoUtility.getFormatDate(scheduler.getStartTime(),currentFormat,targetFormat)+"Z");
				preparedStatement.setString(2, ZDPDaoUtility.getFormatDate(scheduler.getEndTime(),currentFormat,targetFormat)+"Z");
				preparedStatement.setInt(3, scheduler.getFrequency());
				preparedStatement.setString(4, scheduler.getRepeats());
				preparedStatement.setString(5, scheduler.getType());
				preparedStatement.setString(6, scheduler.getTranformType());
				
				
				preparedStatement.setString(7, scheduler.getDataset());
				preparedStatement.setString(8, scheduler.getStatus());
				
				preparedStatement.setInt(9, scheduler.getProjectId());
				
				
				int executeUpdate = preparedStatement.executeUpdate();
				if (executeUpdate > 0) {
					logger.info(
							"Scheduler information succesfully updated against project id" + scheduler.getProjectId());
				}
			} else {
				if (!(StringUtils.isEmpty(scheduler.getStartTime()) || StringUtils.isEmpty(scheduler.getEndTime()))) {
					final String query = "insert into scheduler(startTime,endTime,frequency,project_id,repeats,type,tranformType,dataset,status) values(?,?,?,?,?,?,?,?,?)";
					scheduler.setStatus("New");
					preparedStatement = conn.prepareStatement(query);

					preparedStatement.setString(1, ZDPDaoUtility.getFormatDate(scheduler.getStartTime(),currentFormat,targetFormat)+"Z");
					preparedStatement.setString(2, ZDPDaoUtility.getFormatDate(scheduler.getEndTime(),currentFormat,targetFormat)+"Z");
					preparedStatement.setInt(3, scheduler.getFrequency());
					preparedStatement.setInt(4, scheduler.getProjectId());
					preparedStatement.setString(5, scheduler.getRepeats());
					preparedStatement.setString(6, scheduler.getType());
					preparedStatement.setString(7, scheduler.getTranformType());
					
					preparedStatement.setString(8, scheduler.getDataset());
					preparedStatement.setString(9, scheduler.getStatus());
					
					preparedStatement.execute();
				}
			}
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
			logger.error("Failed to insert or update in schduler!!!" + sqlException.getMessage());
		} finally {

			ConnectionUtility.releaseConnectionResources(preparedStatement, conn);
		}
	}
	
	

	/**
	 * @author Shishir.Sarkar
	 * @param project_id
	 * @return ZDPScheduler
	 */
	@Override
	final public ZDPScheduler getScheduler(final long project_id) {
		String query = "SELECT id,startTime,endTime,frequency,repeats,type,project_Id,tranformType,dataset,status from scheduler where project_id=?;";
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		ResultSet resultSet = null;
		
		String currentDateFormat = "yyyy-MM-dd'T'HH:mm'Z'";  
		String targetFormat = "dd/MM/yyyy h:mm";
		ZDPScheduler zdpScheduler = new ZDPScheduler();
		
        
		try {
			connection = ConnectionUtility.getConnection();
			prepareStatement = connection.prepareStatement(query);
			prepareStatement.setLong(1, project_id);
			resultSet = prepareStatement.executeQuery();			
			
			while (resultSet.next()) {
				zdpScheduler.setId(resultSet.getInt("id"));
				String endTime = ZDPDaoUtility.getFormatDate(resultSet.getString("endTime"), currentDateFormat, targetFormat);
				
				zdpScheduler.setStartTime(ZDPDaoUtility.getFormatDate(resultSet.getString("startTime"), currentDateFormat, targetFormat));
				zdpScheduler.setEndTime(endTime);				
				zdpScheduler.setFrequency(resultSet.getInt("frequency"));
				zdpScheduler.setRepeats(resultSet.getString("repeats"));
				zdpScheduler.setType(resultSet.getString("type"));
				zdpScheduler.setProjectId(resultSet.getInt("project_Id"));
				zdpScheduler.setTranformType(resultSet.getString("tranformType"));
				zdpScheduler.setDataset(resultSet.getString("dataset"));
				String status = resultSet.getString("status")==null?"New" : resultSet.getString("status");
				
				if(status.equals("Active")){
					SimpleDateFormat sdf = new SimpleDateFormat(targetFormat);	
					SimpleDateFormat sdf1 = new SimpleDateFormat(targetFormat);	
					//sdf1.setTimeZone(TimeZone.getTimeZone("GMT"));
					Date date = new Date();
					String currentDate = sdf1.format(date); 
					
					//System.out.println();
					
					try {
						if(sdf.parse(currentDate).after(sdf.parse(endTime))){
							zdpScheduler.setStatus("Expired");
							updateSchedulerStatus("Expired",project_id);
						}
						else{
							zdpScheduler.setStatus(status);
						}
					}
					 catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else{
				zdpScheduler.setStatus(status);
				}
				
			}			
			
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet, prepareStatement, connection);
		}
		return zdpScheduler;
	}
	
	final public String getSchedulerProjectId(String dataset) {
		String query = "SELECT project_id from scheduler where dataset =?;";
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		ResultSet resultSet = null;
		String projectId = "";
		
        
		try {
			connection = ConnectionUtility.getConnection();
			prepareStatement = connection.prepareStatement(query);
			prepareStatement.setString(1, dataset);
			resultSet = prepareStatement.executeQuery();			
			
			while (resultSet.next()) {
				projectId = resultSet.getString("project_id");
			}			
			
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet, prepareStatement, connection);
		}
		return projectId;
	}
	
	public void updateSchedulerStatus(String status,Long project_id){
		final String query = "update scheduler set status=? where project_id=? ";
		Connection connection = null;
		PreparedStatement prepareStatement = null;
		
		try {
			connection = ConnectionUtility.getConnection();
			prepareStatement = connection.prepareStatement(query);
			prepareStatement.setString(1, status);
			prepareStatement.setInt(2, project_id.intValue());
			prepareStatement.executeUpdate();
			
			
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(prepareStatement, connection);
		}
	}
	@Override
	/*
	 * final public String getDataSetFromProject(final Long projectId, final
	 * Integer version) {
	 * 
	 * final String sql =
	 * "SELECT properties FROM zeas.module where project_id = ? and version = ?;"
	 * ; // PreparedStatement ps = null; ResultSet rs = null; Connection conn =
	 * null;
	 * 
	 * String details = null; try { conn = ConnectionUtility.getConnection(); ps
	 * = conn.prepareStatement(sql); ps.setLong(1, projectId); ps.setInt(2,
	 * version); rs = ps.executeQuery(); while (rs.next()) { details =
	 * rs.getString("properties"); } } catch (Exception ex) { try { throw new
	 * Exception(ex); } catch (Exception e1) { e1.printStackTrace(); } } finally
	 * { ConnectionUtility.releaseConnectionResources(rs, ps, conn); } return
	 * details; }
	 * 
	 */
	// change return type to list object(0) - datasetName Object(1) project Name
	public List<String> getDataSetFromProject(Long projectId, Integer version) {
		List<String> projectList = new ArrayList<String>();
		String sql ="";
		if(version==0){
			 sql = "SELECT name,design FROM project where project.id= ? and version = (SELECT MAX(version) from project where project.id=?)";
		}
		else{
			 sql = "SELECT name,design FROM project where project.id= ? and version =?";
		}
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;

		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setLong(1, projectId);
			if(version==0)
				ps.setLong(2, projectId);
			else
				ps.setInt(2, version);
			rs = ps.executeQuery();
			if (rs.next()) {
				projectList.add(getDateSet(rs.getString("design")));
				projectList.add(rs.getString("name"));
			}
		} catch (Exception ex) {
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}

		return projectList;

	}

	private String getDateSet(String design) {

		String DataSet = null;

		String name = null;

		try {

			JSONObject jsonObject = new JSONObject(design).getJSONObject("ExecutionGraph");

			JSONArray json = (JSONArray) jsonObject.get("nodes");

			for (int i = 0; i < json.length(); i++) {

				JSONObject obj = json.getJSONObject(i);

				String s = obj.getString("nodetype");

				if (s.equalsIgnoreCase("Dataset"))

					name = obj.getString("name");

				DataSet = name.substring(0, name.lastIndexOf("_DataSet"));

			}

		} catch (JSONException e) {

			e.printStackTrace();

		}

		return DataSet;

	}

	@Override
	final public String getJSONFromEntity(final String datasetName) {

		final String sql = "SELECT JSON_DATA FROM entity where name=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		String json = null;

		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, datasetName);
			rs = ps.executeQuery();
			if (rs.next()) {
				json = rs.getString("JSON_DATA");
			}
		} catch (Exception ex) {
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return json;
	}

	final public String getCSVFIlePath(final String datasetName) {
		try {
			JSONObject jsonObject = new JSONObject(getJSONFromEntity(datasetName));
			JSONObject jsonObject2 = jsonObject.getJSONObject("fileData");
			return jsonObject2.get("fileName").toString();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	final public String getCSVFIleLocation(final String datasetName) {
		try {
			JSONObject jsonObject = new JSONObject(getJSONFromEntity(datasetName));
			return jsonObject.get("location").toString();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	final public String getProjectTypeDetail(final int projectId) {
		String sql = "SELECT design FROM project where id=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		String json = null;

		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setInt(1, projectId);
			rs = ps.executeQuery();
			if (rs.next()) {
				json = rs.getString("design");
			}
			return json != null ? new JSONObject(json).get("project_type").toString() : null;

		} catch (SQLException | JSONException ex) {
			logger.error("Exception caught in ZDPDataAccessObjectImpl in getProjetTypeDetail: " + ex.getMessage());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
			sql = json = null;
		}

		return null;
	}

	
	public static void main(String[] args) throws ParseException  {
		
		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		System.out.println(dao.getSchedulerProjectId("first10_Ingestion"));
		
		String targetFormat = "dd/MM/yyyy h:mm";
		SimpleDateFormat sdf = new SimpleDateFormat(targetFormat);	
		SimpleDateFormat sdf1 = new SimpleDateFormat(targetFormat);	
		String endTime = "06/12/2016 5:18";
		//sdf1.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date date = new Date();
		String currentDate = sdf1.format(date); 
		System.out.println("current :"+currentDate);
		System.out.println("end :"+endTime);
		
		
		try {
			System.out.println(sdf.parse(currentDate).after(sdf.parse(endTime)));
				
			}
			catch(Exception e){
				
			}
		/*String startdate = "24/11/2016 11:50";
		String enddate = "23/11/2016 11:50";
		
		Date date = new Date();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd h:m");
		System.out.println(sdf.format(date));*/
		
		/*String reformattedStr ="";
		SimpleDateFormat fromUser = new SimpleDateFormat("dd/mm/yyyy HH:mm:ss");
		SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm");

		try {

			//System.out.println(myFormat.format(fromUser.parse(date)));
			reformattedStr = myFormat.format(fromUser.parse(date));
		} catch (ParseException e) {
		    e.printStackTrace();
		}
		//System.out.println(reformattedStr + "Z");
		
		//System.out.println(getFormatDate(date));
*/		
		//System.out.println(getFormatDate(date));

		/*ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		System.out.println("here");
		List<ModuleSchema> m1 = dao.getColumnAndDatatype("9065");
		StringBuilder builder = new StringBuilder();
		for(ModuleSchema m:m1){
			builder.append(m.getName());
			builder.append("  ");
			builder.append(m.getDataType());
			builder.append(",");
		}
		builder.substring(0,builder.lastIndexOf(",")-1);
		System.out.println(builder.substring(0,builder.lastIndexOf(",")));*/
		/*System.out.println(dao.getCSVFIlePath("FlightDelayData_Source"));
		
		//String temp = "/home/zeas/demo/csv/1997.csv";
		String[] split = dao.getCSVFIlePath("FlightDelayData_Source").split("/");
		System.out.println("Filename: "+split[split.length-1]);
		//System.out.println(temp);
*/		
		/*
		 * ZDPScheduler zdpScheduler = new ZDPScheduler();
		 * 
		 * zdpScheduler.setStartTIme("14/10/2016 12:05");
		 * zdpScheduler.setEndTime("14/10/2016 18:05");
		 * zdpScheduler.setFrequency(1); zdpScheduler.setRepeats("Daily");
		 * zdpScheduler.setProjectId(1807);
		 * 
		 * dao.persistScedulerDetail(zdpScheduler);
		 */
		// {"dataIngestionId":"profile_1_component_DataSet","name":"profile_1_component_DataSet","Schema":"profile_1_component","location":"/user/zeas/mat_prof1/profile_1/zeas/component"}
		
		// System.out.println("Data persisted successfully:
		// "+persistScedulerDetail);
		/*
		 * Map<Long, String> schemaMap = null;
		 * 
		 * String outPutLocation = null; String outPutType = null;
		 * 
		 * List<String> schemaList = new LinkedList<>();
		 * 
		 * Long modelId = null; String properties = null; Long l =
		 * Long.parseLong("1322"); try { schemaMap = dao.getProjectSchema(l);
		 * 
		 * for (Map.Entry<Long, String> entry : schemaMap.entrySet()) { modelId
		 * = entry.getKey(); properties = entry.getValue(); //
		 * System.out.println(entry.getKey() + "/" + entry.getValue());
		 * 
		 * } // System.out.println("properties :"+properties);
		 * 
		 * JSONObject jsonObject = new
		 * JSONObject(properties).getJSONObject("params");
		 * 
		 * JSONArray json = (JSONArray) jsonObject.get("columnList");
		 * 
		 * for (int i = 0; i < json.length(); i++) { JSONObject obj =
		 * json.getJSONObject(i); schemaList.add(obj.getString("name")); }
		 * Map<String, String> m = dao.getProjectOuputDetails(modelId);
		 * 
		 * for (Map.Entry<String, String> entry : m.entrySet()) { outPutType =
		 * entry.getKey(); outPutLocation = entry.getValue(); }
		 * 
		 * System.out.println("SchemaList : " + schemaList); System.out.println(
		 * "outPutType  : " + outPutType + ": outPutLocation :" +
		 * outPutLocation);
		 * 
		 * } catch (SQLException e) { e.printStackTrace(); } catch
		 * (JSONException e) { e.printStackTrace(); }
		 */

	}

	@Override
	public String getdatasetIdByName(String datasetName) {
		
		String sql = "select  id from entity where name =?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		String datasetId = null;

		try {
			conn = ConnectionUtility.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, datasetName);
			rs = ps.executeQuery();
			if (rs.next()) {
				datasetId = rs.getString("id");
			}			

		} catch (SQLException ex) {
			logger.error("Exception caught in ZDPDataAccessObjectImpl in getProjetTypeDetail: " + ex.getMessage());
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}

		
		return datasetId;
	}

}
