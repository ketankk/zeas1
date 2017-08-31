package com.itc.zeas.utility.utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.zeas.ingestion.model.ZDPIngestionRunInfo;
import com.zdp.dao.ZDPModule;
import com.zdp.dao.ZDPModuleHistory;
import com.itc.zeas.project.model.ZDPProject;
import com.itc.zeas.project.model.ZDPProjectHistory;
import com.zdp.dao.ZDPWorkspace;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.project.extras.QueryConstants;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.project.model.ProjectEntity;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ZDPDaoUtility {

	public static String getTableName(String objectType) {
		String tableName = "";

		switch (objectType) {

		case ZDPDaoConstant.ZDP_ENTITY_TABLE:
			tableName = ZDPDaoConstant.ZDP_ENTITY_TABLE;
			break;
		case ZDPDaoConstant.ZDP_PROJECT_TABLE:
			tableName = ZDPDaoConstant.ZDP_PROJECT_TABLE;
			break;

		case ZDPDaoConstant.ZDP_MODULE_TABLE:
			tableName = ZDPDaoConstant.ZDP_MODULE_TABLE;
			break;

		case ZDPDaoConstant.ZDP_WORKSPACE:
			tableName = ZDPDaoConstant.ZDP_WORKSPACE;
			break;
		case ZDPDaoConstant.ZDP_PROJECT_HISTORY:
			tableName = ZDPDaoConstant.ZDP_PROJECT_HISTORY;
			break;
		case ZDPDaoConstant.ZDP_MODULE_HISTORY:
			tableName = ZDPDaoConstant.ZDP_MODULE_HISTORY;
			break;
		case ZDPDaoConstant.ZDP_INGESTION_RUN_INFO:
			tableName = ZDPDaoConstant.ZDP_INGESTION_RUN_INFO;
			break;
		}

		return tableName;
	}

	public static Map<String, String> getColumnAndType(String objectType,
			ProjectEntity projectEntity) {
		Map<String, String> columnNameAndType = null;
		int version=0;
		if(projectEntity.getVersion()!=null)
			 version=Integer.parseInt(projectEntity.getVersion());
		
		switch (objectType) {

		
		case "project":
			ZDPProject project = new ZDPProject(projectEntity.getId(),
					projectEntity.getName(), projectEntity.getJsonblob(),
					version, projectEntity.getCreated(),
					projectEntity.getCreatedBy(), projectEntity.getWorkspace_name());
			columnNameAndType = project.getColumnNameAndType();
			break;

		case "module":
			ZDPModule transformation = new ZDPModule(projectEntity.getId(),
					projectEntity.getName(), projectEntity.getJsonblob(),
					version, projectEntity.getProject_id(),
					projectEntity.getCreated(), projectEntity.getCreatedBy());
			columnNameAndType = transformation.getColumnNameAndType();
			break;

		case "workspace":
			ZDPWorkspace workspace = new ZDPWorkspace(projectEntity.getId(),
					projectEntity.getName(), projectEntity.getCreated(),
					projectEntity.getCreatedBy());
			columnNameAndType = workspace.getColumnNameAndType();
			break;
		case "project_history":
			ZDPProjectHistory projectHistory = new ZDPProjectHistory(
					projectEntity.getId(), projectEntity.getProject_id(),
					version, projectEntity.getRun_mode(),
					projectEntity.getStart_time(), projectEntity.getEnd_time(),
					projectEntity.getCreatedBy(), projectEntity.getStatus(),
					projectEntity.getRun_details(), projectEntity.getOozie_id());
			columnNameAndType = projectHistory.getColumnNameAndType();
			break;

		case "module_history":
			ZDPModuleHistory moduleHistory = new ZDPModuleHistory(
					projectEntity.getId(), projectEntity.getModule_id(), version,
					projectEntity.getProject_run_id(), projectEntity.getOutput_blob(),
					projectEntity.getStart_time(), projectEntity.getEnd_time(),
					projectEntity.getCreatedBy(), projectEntity.getStatus(),
					projectEntity.getDetails(), projectEntity.getOozie_id());
			columnNameAndType = moduleHistory.getColumnNameAndType();
			break;
		case "ingestion_run_info":
			ZDPIngestionRunInfo ingestionRunInfo = new ZDPIngestionRunInfo(
					projectEntity.getId(), projectEntity.getMd5(), projectEntity.getFilename(),
					projectEntity.getSchemaname(), projectEntity.getNoofrecord(),
					projectEntity.getCreated(), projectEntity.getCreatedBy());
			columnNameAndType = ingestionRunInfo.getColumnNameAndType();
			break;
		}
		return columnNameAndType;
	}

	public static Map<String, String> getColumnAndValue(String objectType,
			ProjectEntity projectEntity) {
		Map<String, String> columnNameAndValue = null;
		int version=0;
		if(projectEntity.getVersion()!=null)
			 version=Integer.parseInt(projectEntity.getVersion());
		
		switch (objectType) {

		case "project":
			ZDPProject project = new ZDPProject(projectEntity.getId(),
					projectEntity.getName(), projectEntity.getJsonblob(),
					version, projectEntity.getCreated(),
					projectEntity.getCreatedBy(), projectEntity.getWorkspace_name());
			project.getColumnNameAndType();
			columnNameAndValue = project.getColumnNameAndValue();
			break;
		case "module":
			ZDPModule module = new ZDPModule(projectEntity.getId(), projectEntity.getName(),
					projectEntity.getJsonblob(),version,
					projectEntity.getProject_id(), projectEntity.getCreated(),
					projectEntity.getCreatedBy());
			columnNameAndValue = module.getColumnNameAndValue();
			break;

		case "workspace":
			ZDPWorkspace workspace = new ZDPWorkspace(projectEntity.getId(),
					projectEntity.getName(), projectEntity.getCreated(),
					projectEntity.getCreatedBy());
			columnNameAndValue = workspace.getColumnNameAndValue();
			break;

		case "project_history":
			ZDPProjectHistory projectHistory = new ZDPProjectHistory(
					projectEntity.getId(), projectEntity.getProject_id(),
					version, projectEntity.getRun_mode(),
					projectEntity.getStart_time(), projectEntity.getEnd_time(),
					projectEntity.getCreatedBy(), projectEntity.getStatus(),
					projectEntity.getRun_details(), projectEntity.getOozie_id());
			columnNameAndValue = projectHistory.getColumnNameAndValue();
			break;

		case "module_history":
			ZDPModuleHistory moduleHistory = new ZDPModuleHistory(
					projectEntity.getId(), projectEntity.getModule_id(), version,
					projectEntity.getProject_run_id(), projectEntity.getOutput_blob(),
					projectEntity.getStart_time(), projectEntity.getEnd_time(),
					projectEntity.getCreatedBy(), projectEntity.getStatus(),
					projectEntity.getDetails(), projectEntity.getOozie_id());
			columnNameAndValue = moduleHistory.getColumnNameAndValue();
			break;
		case "ingestion_run_info":
			ZDPIngestionRunInfo ingestionRunInfo = new ZDPIngestionRunInfo(
					projectEntity.getId(), projectEntity.getMd5(), projectEntity.getFilename(),
					projectEntity.getSchemaname(), projectEntity.getNoofrecord(),
					projectEntity.getCreated(), projectEntity.getCreatedBy());
			columnNameAndValue = ingestionRunInfo.getColumnNameAndValue();
			break;
		}

		return columnNameAndValue;
	}

	public static String formCreateQuery(String tableName,
			Map<String, String> columnNameAndType) {

		StringBuilder columnList = new StringBuilder();
		StringBuilder inputList = new StringBuilder();
		int length = columnNameAndType.size();
		int count = 1;
		for (Entry<String, String> entry : columnNameAndType.entrySet()) {
			columnList.append(entry.getKey());
			inputList.append("?");
			if (count < length) {
				columnList.append(",");
				inputList.append(",");
				count++;
			}
		}
		String query = "insert into " + tableName + "(" + columnList
				+ ") values(" + inputList + ")";
		return query;

	}

	public static long getSequenceId(String idType) {

		long id = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = ConnectionUtility.getConnection();
			if (ZDPDaoConstant.HISTORY.equalsIgnoreCase(idType)) {
				ps = conn.prepareStatement(
						QueryConstants.PROJECT_HISTORY_ID_QUERY,
						PreparedStatement.RETURN_GENERATED_KEYS);
			} else {
				ps = conn.prepareStatement(QueryConstants.SEQUENCE_QUERY,
						PreparedStatement.RETURN_GENERATED_KEYS);
			}
			ps.execute();
			rs = ps.getGeneratedKeys();
			while (rs.next()) {
				id = rs.getLong(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(rs, ps, conn);
		}
		return id;
	}



	// convert hive datatype to mysql
	// changes the previous implementation which was using map. 
	public static String getSQLDatatypeFromHive(String dataType) {

		String type="varchar(4000)";
		if(dataType !=null ){
			
			if(dataType.contains("string")){
				type="varchar(4000)";
			}
			else if(dataType.contains("long")){
				type="bigint(20)";
			}
			else if(dataType.contains("int")){
				type="int(15)";
			}
			else if(dataType.contains("double")){
				type="double";
			}
			else if(dataType.contains("date")){
				type="date";
			}
			else if(dataType.contains("timestamp")){
				type="timestamp";
			}
		}
		return type;
	}

	// convert mysql datatype to hive
	// changes the previous implementation which was using map.
	public static String getHiveFromSQLDatatype(String dataType) {

		String type="string";
		if(dataType !=null ){
			
			if(dataType.contains("varchar")){
				type="string";
			}
			else if(dataType.contains("bigint")){
				type="long";
			}
			else if(dataType.contains("int")){
				type="int";
			}
			else if(dataType.contains("double")){
				type="double";
			}
			else if(dataType.contains("decimal")){
				type="double";
			}
			else if(dataType.contains("date")){
				type="date";
			}
			else if(dataType.contains("timestamp")){
				type="timestamp";
			}
		}
		return type;
	}

	// return column and datatype array for column list
	public static JSONArray getJSONArray(Map<String, String> objs) {

		JSONArray array = new JSONArray();
		List<JSONObject> columnList = new ArrayList<>();

		for (Map.Entry<String, String> entry : objs.entrySet()) {
			JSONObject column = new JSONObject();
			try {
				column.put("name", entry.getKey());
				column.put("dataType", entry.getValue());
				array.put(column);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return array;

	}

	/*
	 * return the hhmmss from values from long number
	 */
	public static String getTimeInHHMMSS(Long seconds) {

		Long dd = 0l;
		Long hh = 0l;
		Long mm = 0l;
		Long ss = 0l;
		if (seconds > 0) {
			mm = seconds / 60;
			ss = seconds % 60;
		}
		if (mm > 0) {
			hh = mm / 60;
			mm = mm % 60;
		}
		if (hh > 0) {
			dd = hh / 24;
			hh = hh % 24;
		}
		StringBuilder ddhhmmss = new StringBuilder();
		if (dd > 0) {

			ddhhmmss.append(dd + "d ");
		}
		if (hh > 0) {
			String str = "";
			if (hh < 9 && hh > 0) {
				str = "0";
			}
			ddhhmmss.append(str + hh + ":");
		} else {
			ddhhmmss.append("00:");
		}
		if (mm > 0) {
			String str = "";
			if (mm < 9 && mm > 0) {
				str = "0";
			}
			ddhhmmss.append(str + mm + ":");
		} else {
			ddhhmmss.append("00:");
		}
		if (ss > 0) {
			String str = "";
			if (ss < 9 && ss > 0) {
				str = "0";
			}
			ddhhmmss.append(str + ss);
		} else {
			ddhhmmss.append("00");
		}

		return ddhhmmss.toString();
	}

	// get module ids and version list for given project json
	public static Set<String> getModuleIdsAndVersion(String json) {

		ObjectMapper mapper = new ObjectMapper();
		Set<String> list = new HashSet<String>();
		JsonNode jsonNode;
		try {
			jsonNode = mapper.readTree(json);
			JsonNode stagelist = jsonNode.get("stageList");
			JsonNode stages = stagelist.get("stages");
			Iterator<JsonNode> stageItr = stages.iterator();
			while (stageItr.hasNext()) {
				JsonNode temp = stageItr.next();
				String s = temp.get("stageName").textValue().trim();
				String[] arr = null;
				if (s != null) {
					arr = s.split(",");
					list.addAll(Arrays.asList(arr));
				}
				String s1 = temp.get("input").textValue().trim();
				if (s1 != null) {
					arr = s.split(",");
					list.addAll(Arrays.asList(arr));
				}
				String s2 = temp.get("output").textValue().trim();
				if (s2 != null) {
					arr = s.split(",");
					list.addAll(Arrays.asList(arr));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

	/*
	 * Returns the object type on given input
	 */
	public static String getType(String type) {

		String objectType = ZDPDaoConstant.ZDP_MODULE_TABLE;
		switch (type) {
		case "DataIngestion":
		case "DataSet":
		case "DataSchema":
		case "DataSource":
			objectType = ZDPDaoConstant.ZDP_ENTITY_TABLE;
			break;
		}
		return objectType;
	}
public static  String getFormatDate(String currentdate,String currentFormat,String targetFormat){		
		
		String dateString = currentdate;//"2016-10-28T11:55Z";
		String targetDate ="";
		Date date;
		try {
			date = new SimpleDateFormat(currentFormat).parse(dateString);
			 targetDate = new SimpleDateFormat(targetFormat).format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return targetDate;
	}
}
