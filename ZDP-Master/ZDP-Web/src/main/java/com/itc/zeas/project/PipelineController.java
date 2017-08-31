package com.itc.zeas.project;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.utility.CommonUtils;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.utility.PipelineControllerUtility;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.ingestion.automatic.rdbms.SqoopImportDetails;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.pipeline.WorkflowBuilder;
import com.itc.zeas.model.ModuleSchema;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.ingestion.model.ZDPScheduler;
import com.itc.zeas.exceptions.SqlIoException;
import com.itc.zeas.ingestion.model.OozieJob;
import com.itc.zeas.ingestion.model.OozieStageStatusInfo;

/**
 * @author 11786
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class PipelineController {

	PipelineControllerUtility pipelineControllerUtility = new PipelineControllerUtility();
	private static final Logger LOGGER = Logger.getLogger(PipelineController.class);

	private static Long modelId = null;
	private static String properties = null;

	private ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();

	private String APP_PATH; // /home/zeas/zeas/Config/<dataset_name>
	private String SQOOP_APP_PATH;



	@RequestMapping(value = "/project/ingestion/{projectId}/{version}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<String> killScheduler(@PathVariable("projectId") Long projectId,
			@PathVariable("version") Integer version, HttpServletRequest httpRequest) {
		ResponseEntity<String> responseEntity = null;

		WorkflowBuilder wf = new WorkflowBuilder();
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		Boolean haveValidPermission = false;
		// IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
		try {
			haveValidPermission = zdpUserAccessImpl.validateUserPermissionForResource(
					UserManagementConstant.ResourceType.PROJECT, userName, projectId,
					UserManagementConstant.READ_EXECUTE);

		} catch (SqlIoException.IoException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (SqlIoException.SqlException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (haveValidPermission) {
			try {
				List<String> projectList = dao.getDataSetFromProject(projectId, version);
				String datasetName = projectList.get(0);
				String projectName = projectList.get(1);

				String workflowName = projectName + "_" + datasetName;
				/*
				 * String[] args = new String[3]; args[0] =
				 * ShellScriptExecutor.BASH; args[1] =
				 * System.getProperty("user.home") + "/zeas/Config/killJob.sh";
				 * args[2] = workflowName;
				 * 
				 * ShellScriptExecutor shExe = new ShellScriptExecutor(); int
				 * status = shExe.runScript(args);
				 */

				String strArry[] = new String[3];
				String SQOOP_SCRIPT_PATH = System.getProperty("user.home") + "/zeas/Config/killJob.sh";
				String SHELL_SCRIPT_TYPE = "/bin/bash";
				strArry[0] = SHELL_SCRIPT_TYPE;
				strArry[1] = SQOOP_SCRIPT_PATH;
				strArry[2] = workflowName;

				ShellScriptExecutor shExe = new ShellScriptExecutor();
				shExe.runScript(strArry);

				// LOGGER.debug("status of kill script : {} "+ status);

				// check if killed otherwise don't update status
				dao.updateSchedulerStatus("Killed", projectId);

			} catch (Exception exception) {
				responseEntity = new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
			}
		}
		return responseEntity = new ResponseEntity<String>("success", HttpStatus.OK);
	}

	/**
	 * @param projectId
	 * @param version
	 * @param httpRequest
	 * @return ResponseBody
	 */
	@RequestMapping(value = "/project/ingestion/{projectId}/{version}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<String> executeProject(@PathVariable("projectId") Long projectId,
			@PathVariable("version") Integer version, HttpServletRequest httpRequest) {
		ResponseEntity<String> responseEntity = null;

		WorkflowBuilder wf = new WorkflowBuilder();
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		Boolean haveValidPermission = false;
		// IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
		try {
			haveValidPermission = zdpUserAccessImpl.validateUserPermissionForResource(
					UserManagementConstant.ResourceType.PROJECT, userName, projectId,
					UserManagementConstant.READ_EXECUTE);

		} catch (SqlIoException.IoException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (SqlIoException.SqlException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String currentFormat = "dd/MM/yyyy HH:mm";
		String targetFormat = "yyyy-MM-dd'T'HH:mm";

		if (haveValidPermission) {
			try {
				List<String> projectList = dao.getDataSetFromProject(projectId, version);

				String datasetName = projectList.get(0);
				String projectName = projectList.get(1);

				String sourceType = commonUtils.getSourceType(datasetName + "_Schedular").toLowerCase();

				ZDPScheduler scheduler = dao.getScheduler(projectId);

				String datasetId = dao.getdatasetIdByName(datasetName);

				List<ModuleSchema> schema = dao.getColumnAndDatatype(datasetId);

				String schemaString = pipelineControllerUtility.schemaToString(schema);

				final String startTime = pipelineControllerUtility.getFormatDate(scheduler.getStartTime(),
						currentFormat, targetFormat) + "Z";

				String endTime = pipelineControllerUtility.getFormatDate(scheduler.getEndTime(), currentFormat,
						targetFormat) + "Z";

				final String frequency = PipelineControllerUtility
						.constructFrequency(Integer.toString(scheduler.getFrequency()), scheduler.getRepeats());

				final String timeZone = "GMT";// PipelineControllerUtility.getDefaultTimeZone();
				if (scheduler.getType().equalsIgnoreCase("time_based")) {
					switch (sourceType.toLowerCase()) {
					case "rdbms": {
						SqoopImportDetails sqoop = new SqoopImportDetails();

						if (isScriptExist(sqoop, datasetName)) {
							/**
							 * Create Coordinator builder based on scheduler
							 * details from scheduler table Create work-flow
							 * builder call existing script into work-flow
							 * builder
							 */

							sqlIngestion(wf, projectName, datasetName, startTime, endTime, frequency, timeZone,
									schemaString);
						} else {
							/**
							 * Create Coordinator builder based on scheduler
							 * details from scheduler table Create work-flow
							 * builder call existing script into work-flow
							 * builder
							 */
							sqoop.getDetailsForImport(datasetName + "_Schedular");
							sqlIngestion(wf, projectName, datasetName, startTime, endTime, frequency, timeZone,
									schemaString);
						}
					}
					case "file": {
						performFileIngestion(wf, projectName, datasetName, startTime, endTime, frequency, timeZone,
								schemaString);
					}
					}
				}
			} catch (Exception e) {

			}

			// Hive logic should go here
			dao.updateSchedulerStatus("Active", projectId);
		}
		APP_PATH = null;

		return responseEntity = new ResponseEntity<String>("success", HttpStatus.OK);
	}



	@RequestMapping(value = "/project/schedulerStatus/{projectId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody Map<String, String> schedulerStatus(@PathVariable("projectId") Long projectId)
			throws IOException {

		Map<String, String> statusReport = new HashMap<String, String>();
		ZDPScheduler scheduler = dao.getScheduler(projectId);
		statusReport.put("projectId", Long.toString(projectId));

		statusReport.put("dataset", scheduler.getDataset());
		statusReport.put("status", scheduler.getStatus());

		return statusReport;

	}

	@RequestMapping(value = "/listPipelineStageLogDetails/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody List<OozieStageStatusInfo> listPipelineStageLogDetails(
			@PathVariable("entityId") Integer entityId) {
		List<OozieStageStatusInfo> oozieStageStatusInfoList = new ArrayList<OozieStageStatusInfo>();

		String oozieJobId = PipelineJobInfoController.getOozieJobId(String.valueOf(entityId));
		OozieJob oozieJob = PipelineJobInfoController.getOozieJobInfo(oozieJobId);

		oozieStageStatusInfoList = oozieJob.getOozieStageStatusInfoList();
		return oozieStageStatusInfoList;
	}


	private List<String> getSchemaFromJSON(ZDPDataAccessObjectImpl dao, Long projectId) {

		Map<Long, String> schemaMap = null;

		List<String> schemaList = new LinkedList<>();

		try {
			schemaMap = dao.getProjectSchema(projectId);
			for (Map.Entry<Long, String> entry : schemaMap.entrySet()) {
				modelId = entry.getKey();
				properties = entry.getValue();
			}

			if (properties != null) {
				JSONObject jsonObject = new JSONObject(properties).getJSONObject("params");

				JSONArray json = (JSONArray) jsonObject.get("columnList");

				for (int i = 0; i < json.length(); i++) {
					JSONObject obj = json.getJSONObject(i);
					schemaList.add(obj.getString("name"));
					System.out.println("Name : " + obj.getString("name") + "DataType :" + obj.getString("dataType"));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return schemaList;
	}

	/**
	 * 
	 * @return response Copied successfully if HDFS files is moved successfully
	 *         to TEMPs location
	 */
	@RequestMapping(value = "/doCopyHDFSOutputFile", method = RequestMethod.POST, headers = "Accept=application/json")
	final public @ResponseBody String doCopyHDFSOutputFile(@PathVariable("projectId") Long projectId) {
		String response = "Failed to copy from HDFS location!!! ";
		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();

		List<String> schemaList = getSchemaFromJSON(dao, projectId);

		final String sourcePath = null;
		/*
		 * HdfsReader hdfsReader = new HdfsReader(); int responseFromHDFSReader
		 * = hdfsReader.startHDFSReader(sourcePath); if (responseFromHDFSReader
		 * != 0) { OutputIngestionResultReader outputIngestionResultReader = new
		 * OutputIngestionResultReader(); List<String> arrayList = new
		 * ArrayList<String>();
		 * outputIngestionResultReader.readIngestionResultFromHDFS(schemaList);
		 * Collection<IngestionResult> ingestionDataIntoList =
		 * outputIngestionResultReader.getIngestionDataIntoList(); for
		 * (IngestionResult ingestionResult : ingestionDataIntoList) {
		 * arrayList.add(Parser.parse(ingestionResult)); }
		 */
		// String[] array = (String[]) arrayList.toArray();
		// return array.toString();

		/*
		 * } else { return response; }
		 */
		return null;
	}

	@RequestMapping(value = "/project/jobstatus/{projectId}/{version}", method = RequestMethod.GET, produces = "application/json", headers = "Accept=application/json")
	public ResponseEntity<String> jobStatus(@PathVariable("projectId") Long projectId,
			@PathVariable("version") Integer version, HttpServletRequest httpRequest) {

		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		List<String> projectList = dao.getDataSetFromProject(projectId, version);

		String datasetName = projectList.get(0);
		String projectName = projectList.get(1);

		String jobName = projectName + "_" + datasetName;
		String oozieURL = ConfigurationReader.getProperty("OOZIE_ENGINE");
		String fileName = System.getProperty("user.home") + "/zeas/Config/script/jobstatus.sh";
		pipelineControllerUtility.getJobStatus(jobName, oozieURL, fileName);
		return null;

	}



	/**
	 * @param wf
	 * @param projectName
	 * @param datasetName
	 * @param startTime
	 * @param endTime
	 * @param frequency
	 * @param timeZone
	 */
	private void performFileIngestion(WorkflowBuilder wf, String projectName, String datasetName, String startTime,
			String endTime, String frequency, String timeZone, String schemaString) {

		// Retriving csv file path.

		String workflowName = projectName + "_" + datasetName;

		final String csvFileName = dao.getCSVFIlePath(datasetName);
		// Split the csv file path & extracting the file name.
		String[] split = csvFileName.split("/");
		APP_PATH = System.getProperty("user.home") + "/zeas/" + projectName + "/" + datasetName;
		// Excuting the hdfs put command to move the csv file from linux
		// location to hdfs location {user/zeas/projectname/dataset}
		// need to change here "/home/zeas/zeas/sample/demo/CCIL_final.csv",
		pipelineControllerUtility.executeHDFSPUTCommand(split[split.length - 1],
				csvFileName.substring(0, csvFileName.lastIndexOf('/')), projectName, datasetName);
		// Credting shell file to run the mapreduce job
		pipelineControllerUtility.doCreateShellFileForMapReduce(projectName, datasetName, split[split.length - 1]);
		Document doc = wf.getOozieWorkFlowTemplate("mapreduce", "mapreduce.sh", projectName + "_" + datasetName,
				PipelineControllerUtility.FILE_TYPE);
		// Saving Workflow xml file
		wf.saveWorkFlowXML(doc, APP_PATH + "/workflow.xml");
		// Excuting the hdfs put command to move the workflow.xml file from
		// linux location to hdfs location {user/zeas/projectname/dataset}
		pipelineControllerUtility.executeHDFSPUTCommand("workflow.xml", APP_PATH, projectName, datasetName);

		// APP_PATH is Linux location from where job is going to
		Document coordinatorDoc = wf.getcoordinatorTemplate(APP_PATH, frequency, workflowName);
		// Saving coordinator xml file in linux path
		wf.saveWorkFlowXML(coordinatorDoc, APP_PATH + "/coordinator.xml");
		// Excuting the hdfs put command to move the coordinator.xml file from
		// linux location to hdfs location {user/zeas/projectname/dataset}
		pipelineControllerUtility.executeHDFSPUTCommand("coordinator.xml", APP_PATH, projectName, datasetName);

		// hive.sh file creation and saving to HDFS
		String tableName = datasetName + "_Dataset";
		String path = pipelineControllerUtility.hiveShellScript(schemaString, tableName, projectName, datasetName);
		pipelineControllerUtility.executeHDFSPUTCommand("hive.sh", path, projectName, datasetName);

		// Creating config file to run the oozie job
		pipelineControllerUtility.doCreatetheCoordinatorConfigPropertiesFIle(startTime, endTime, frequency, datasetName,
				projectName, timeZone, "mapreduce.sh");
		// Creating shell file to run the oozie job
		final String doCreateShellScriptFile = pipelineControllerUtility.doCreateShellScriptFile(datasetName,
				ConfigurationReader.getProperty("OOZIE_ENGINE"), projectName);
		// below statement is used to run the shell file
		pipelineControllerUtility.runShellFile(doCreateShellScriptFile);

	}

	private void sqlIngestion(WorkflowBuilder wf, String projectName, String datasetName, String startTime,
			String endTime, String frequency, String timeZone, String schemaString) {

		APP_PATH = System.getProperty("user.home") + "/zeas/" + projectName + "/" + datasetName;
		String path1 = SQOOP_APP_PATH.replaceAll("SqoopImport.sh", "SchedulerSqoopImport.sh");

		pipelineControllerUtility.executeHDFSPUTCommand("SchedulerSqoopImport.sh", SQOOP_APP_PATH, projectName,
				datasetName);

		String workflowName = projectName + "_" + datasetName;
		Document doc = wf.getOozieWorkFlowTemplate("sqoop", "SchedulerSqoopImport.sh", workflowName,
				PipelineControllerUtility.RDBMS_TYPE);

		String rootPath = APP_PATH;
		File ooziepath = new File(rootPath);
		if (!ooziepath.exists()) {
			ooziepath.mkdirs();
		}
		wf.saveWorkFlowXML(doc, APP_PATH + "/workflow.xml");
		pipelineControllerUtility.executeHDFSPUTCommand("workflow.xml", APP_PATH, projectName, datasetName);

		String appPath = ConfigurationReader.getProperty("HDFS_USER_PATH") + projectName + "/" + datasetName;
		Document coordinatorDoc = wf.getcoordinatorTemplate(appPath, frequency, workflowName);
		wf.saveWorkFlowXML(coordinatorDoc, APP_PATH + "/coordinator.xml");
		pipelineControllerUtility.executeHDFSPUTCommand("coordinator.xml", APP_PATH, projectName, datasetName);

		// hive.sh file creation and saving to HDFS
		String tableName = datasetName + "_Dataset";

		String path = pipelineControllerUtility.hiveShellScript(schemaString, tableName, projectName, datasetName);
		pipelineControllerUtility.executeHDFSPUTCommand("hive.hql", path, projectName, datasetName);
		pipelineControllerUtility.executeHDFSPUTCommand("hive-site.xml",
				System.getProperty("user.home") + "/zeas/Config/", projectName, datasetName);

		//

		pipelineControllerUtility.doCreatetheCoordinatorConfigPropertiesFIle(startTime, endTime, frequency, datasetName,
				projectName, timeZone, "SchedulerSqoopImport.sh");

		final String shellScriptFile = pipelineControllerUtility.doCreateShellScriptFile(datasetName,
				ConfigurationReader.getProperty("OOZIE_ENGINE"), projectName);

		String[] args = new String[2];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = shellScriptFile;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);

		// pipelineControllerUtility.runShellFile(shellScriptFile);
	}

	private boolean isScriptExist(SqoopImportDetails sqoop, String name) {
		// Gets the entity object by scheduler name.

		JSONObject jsonObj = sqoop.getJsonObjectByName(name + "_Schedular");
		String dataSource = jsonObj.getString("dataSource");

		// Retrieve json blob from entity using data source field.
		jsonObj = sqoop.getJsonObjectByName(dataSource);
		String schema = jsonObj.getString("schema");

		String rootPathScript = System.getProperty("user.home") + "/zeas/Config/" + schema;
		File rootPathScriptFile = new File(rootPathScript);
		if (!rootPathScriptFile.exists()) {
			return false;
		}
		SQOOP_APP_PATH = rootPathScript;

		rootPathScript += "/SchedulerSqoopImport.sh";
		rootPathScriptFile = new File(rootPathScript);
		if (!rootPathScriptFile.exists()) {
			return false;
		}
		return true;
	}


}
