package com.itc.zeas.v2.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.pipeline.PipelineUtil;
import com.itc.zeas.project.model.ProjectEntity;
import com.zdp.dao.SearchCriteriaEnum;
import com.itc.zeas.project.model.SearchCriterion;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.project.extras.ZDPDaoConstant;

public class ProjectUtil {

	public static Logger LOG = Logger.getLogger(PipelineUtil.class);

	public static void runOozieWorkflow(String propFilePath,
			String pipeLineName, Long projectRunId, List<Stage> actions,String prName,String userName) {
		/*
		 * ShellScriptExecutor exec = new ShellScriptExecutor(); String[] args =
		 * new String[5]; args[0] = ShellScriptExecutor.BASH; args[1] =
		 * System.getProperty
		 * ("user.home")+"/zeas/Config/triggerOozieWorkflow.sh"; args[2] =
		 * "-oozie "+ConfigurationReader.getProperty("OOZIE_ENGINE"); args[3] =
		 * "-config "+propFilePath; args[4] = "-run"; exec.runScript(args);
		 */
		String line = "";
		String oozieJobId = "";
		BufferedReader br = null;
		try {
			String[] command = {
					ShellScriptExecutor.BASH,
					System.getProperty("user.home")
							+ "/zeas/Config/triggerOozieWorkflow.sh",
					"-oozie " + ConfigurationReader.getProperty("OOZIE_ENGINE"),
					"-config " + propFilePath, "-run" };
			ProcessBuilder processBuilder = new ProcessBuilder(command);

			Process oozieProcess = processBuilder.start();
			 br = new BufferedReader(new InputStreamReader(
					oozieProcess.getInputStream()));
			ZDPDataAccessObject accessObject=new ZDPDataAccessObjectImpl();
			boolean isInserted=false;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("job: ")) {
					oozieJobId = line.substring(5);
					if(!isInserted){
					accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_SCHEDULE, oozieJobId, userName);
					isInserted=true;
					}
					System.out.println("oozieJobId: " + oozieJobId);
					// add oozie id in Project run history table
					addOozieIdInProjectRunHistory(oozieJobId, projectRunId);
					// add module run history
					makeEntryInModuleRunHistory(oozieJobId, projectRunId,
							pipeLineName, actions);
					// Start polling Oozie DB for Job status
					OozieJobStatusPoller status = new OozieJobStatusPoller(
							oozieJobId);
					Thread oozieThread = new Thread(status);
					oozieThread.start();
					break;
				}
			}
			// logs the error
			br = new BufferedReader(new InputStreamReader(
					oozieProcess.getErrorStream()));
			while ((line = br.readLine()) != null) {
				System.out.println("SCRIPT_ERROR:" + line);
			}
			br.close();
		} catch (IOException | SQLException ex) {
			LOG.error("SQLException occured while executing sql select query for inserting oozie job id and pipeline id "
					+ oozieJobId);
			ex.printStackTrace();
			LOG.error("Error invoking Oozie JOB for pipeline - " + pipeLineName);
		}
		finally{
			//closing bufferedReader
			if(br != null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
	}

	public static ProjectEntity getProjectHistoryEntity(
			String projectName, String oozie_id) {

		ProjectEntity projectEntity = new ProjectEntity();
		String project_id = projectName.split("-")[0];
		String version = projectName.split("-")[1];
		projectEntity.setProject_id(Long.parseLong(project_id));
		projectEntity.setVersion(version);
		projectEntity.setOozie_id(oozie_id);
		projectEntity.setRun_mode("manual");
		projectEntity.setSchemaType(ZDPDaoConstant.ZDP_PROJECT_HISTORY);

		return projectEntity;
	}

	public static ProjectEntity getModuleHistoryEntity(Stage stage,
                                                       Long project_run_id, String oozie_id) {

		ProjectEntity projectEntity = new ProjectEntity();
		String moduleIdVersion = stage.getName();
		String module_id = moduleIdVersion.split("-")[0];
		String version = moduleIdVersion.split("-")[1];
		projectEntity.setModule_id(Long.parseLong(module_id));
		projectEntity.setVersion(version);
		projectEntity.setProject_run_id(project_run_id);
		projectEntity.setOutput_blob(buildOutputJSON(stage).toString());
		projectEntity.setOozie_id(oozie_id);
		if (stage.getStageType().equalsIgnoreCase(
				ProjectConstant.HIVE_TRANSFORMATION)) {
			projectEntity.setDetails("Output Hive table " + "m_"
					+ stage.absTransformation.getId().replace("-", "_"));
		} else {
			projectEntity.setDetails("Output HDFS path "
					+ stage.absTransformation.getOutputLocation());
		}
		projectEntity.setSchemaType(ZDPDaoConstant.ZDP_MODULE_HISTORY);

		return projectEntity;
	}

	public static ProjectEntity getIntermediateDatasetEntity(Stage stage,
                                                             String projectName) {

		ProjectEntity projectEntity = new ProjectEntity();
		String project_id = projectName.split("-")[0];
		projectEntity.setProject_id(Long.parseLong(project_id));
		projectEntity.setSchemaType("module");
		String json = buildIntermediateDSJson(stage);
		projectEntity.setJsonblob(json);
		projectEntity.setName("internal dataset");
		return projectEntity;

	}

	/*
	 * public static void makeEntryInProjectRunHistory(String oozieJobId, String
	 * projectName, List<Stage> actions) {
	 * 
	 * ZDPDataAccessObject dataAccessObject = new ZDPDataAccessObjectImpl();
	 * 
	 * // create project run history Entity projectEntity =
	 * getProjectHistoryEntity(projectName, oozieJobId); Entity
	 * projHistempEntity = dataAccessObject.addEntity(projectEntity);
	 * 
	 * for (Stage stage : actions) {
	 * 
	 * // Skipping Fork and Join nodes since these are just Oozie specific. if
	 * (stage.getStageType() .equalsIgnoreCase(ProjectConstant.JOIN_NODE) ||
	 * stage.getStageType().equalsIgnoreCase( ProjectConstant.FORK)) { continue;
	 * } // create intermediate dataset Entity intermediateDatasetEntity =
	 * getIntermediateDatasetEntity( stage, projectName);
	 * 
	 * Entity interTempEntity = dataAccessObject
	 * .addEntity(intermediateDatasetEntity); // create module run history Long
	 * project_run_id = projHistempEntity.getId(); Entity moduleHisEntity =
	 * getModuleHistoryEntity(stage, project_run_id, oozieJobId); Entity
	 * moduleTempEntity = dataAccessObject .addEntity(moduleHisEntity);
	 * 
	 * }
	 */
	/**
	 * add Oozie job id to a project run record in project_history table
	 * 
	 * @param oozieJobId
	 *            oozie job id
	 * @param projectRunId
	 *            project run id
	 */
	public static void addOozieIdInProjectRunHistory(String oozieJobId,
			Long projectRunId) {
		Log.debug("inside method addOozieIdInProjectRunHistory oozieJobId: "
				+ oozieJobId + "projectRunId: " + projectRunId);
		List<SearchCriterion> criterionlist = new ArrayList<>();
		SearchCriterion criterion = new SearchCriterion("id",
				projectRunId.toString(), SearchCriteriaEnum.EQUALS);
		criterionlist.add(criterion);
		Map<String, String> columnNameAndValues = new HashMap<>();
		columnNameAndValues.put("oozie_id", oozieJobId);
		ZDPDataAccessObject dataAccessObject = new ZDPDataAccessObjectImpl();
		try {
			dataAccessObject.updateObject("project_history",
					columnNameAndValues, criterionlist);
		} catch (Exception e) {
			LOG.error("problem while adding oozie job id in project_history table "
					+ e.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Add module information in module_history table for given project run id
	 * 
	 * @param oozieJobId
	 *            oozie job id
	 * @param projectRunId
	 *            project run id
	 * @param projectName
	 *            combination of project name and version
	 * @param actions
	 */
	public static void makeEntryInModuleRunHistory(String oozieJobId,
			Long projectRunId, String projectName, List<Stage> actions) {
		Log.debug("inside method makeEntryInModuleRunHistory oozieJobId: "
				+ oozieJobId + " projectRunId: " + projectRunId
				+ " projectName: " + projectName);
		ZDPDataAccessObject dataAccessObject = new ZDPDataAccessObjectImpl();
		for (Stage stage : actions) {

			// Skipping Fork and Join nodes since these are just Oozie specific.
			if (stage.getStageType()
					.equalsIgnoreCase(ProjectConstant.JOIN_NODE)
					|| stage.getStageType().equalsIgnoreCase(
							ProjectConstant.FORK)) {
				continue;
			}
			// create intermediate dataset
			ProjectEntity intermediateDatasetProjectEntity = getIntermediateDatasetEntity(
					stage, projectName);

			dataAccessObject.addEntity(intermediateDatasetProjectEntity);
			// create module run history
			ProjectEntity moduleHisProjectEntity = getModuleHistoryEntity(stage,
					projectRunId, oozieJobId);
			dataAccessObject.addEntity(moduleHisProjectEntity);
		}

	}

	public static String buildIntermediateDSJson(Stage stage) {
		JSONObject obj = new JSONObject();
		JSONObject params = new JSONObject();
		try {
			params.put("location", stage.getAbsTransformation()
					.getOutputLocation());
			// obj.append("params", params.toString());

			obj.put("params", params.toString());

		} catch (JSONException e) {
			e.printStackTrace();
		}
		return obj.toString();
	}

	public static JSONObject buildOutputJSON(Stage module) {
		JSONObject obj = new JSONObject();
		try {

			JSONArray outputs = new JSONArray();
			List<JSONObject> list = new ArrayList<>();
			JSONObject eachOp = new JSONObject();
			eachOp.put("id", 1);
			String id_Version[] = module.getName().split("-");
			eachOp.put("module_id", id_Version[0]);
			eachOp.put("version", id_Version[1]);
			list.add(eachOp);

			obj.put("outputs", outputs.put(list));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}

	public static JSONArray getJSONArray(Map<String, String> objs) {

		JSONArray array = new JSONArray();
		List<JSONObject> columnList = new ArrayList<>();

		for (Map.Entry<String, String> entry : objs.entrySet()) {
			JSONObject column = new JSONObject();
			try {
				column.put(entry.getKey(), entry.getValue());
				columnList.add(column);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return array.put(columnList);

	}

	/**
	 * This is utility method to check if given parent element can be ignored as
	 * parent.
	 * 
	 * @param projectEntity
	 *            {@link ProjectEntity} object
	 * @return {@link Boolean} true if to be ignored else false
	 */
	public static boolean ignoreParent(ProjectEntity projectEntity) {
		return projectEntity.getName() != null
				&& (projectEntity.getName().equalsIgnoreCase("DATASET")
						|| projectEntity.getName()
								.equalsIgnoreCase("internal dataset")
						|| projectEntity.getName().equalsIgnoreCase(
								ProjectConstant.ML_MULTICLASS) || projectEntity
						.getName().equalsIgnoreCase(
								ProjectConstant.ML_LENEAR_REGRESSION));
	}

	/**
	 * Helper method verifies if this stage need not be added to list of Action
	 * nodes for Oozie processing.
	 * 
	 * @param each
	 *
	 * @return {@link Boolean}
	 */
	public static boolean ignoreProcessingForWorkflow(
			Entity each) {
		return (each.getType() == null
				|| each.getType().equalsIgnoreCase("dataset")
				|| each.getType().equalsIgnoreCase(
						ProjectConstant.ML_LENEAR_REGRESSION) || each.getType()
				.equalsIgnoreCase(ProjectConstant.ML_MULTICLASS));
	}
}
