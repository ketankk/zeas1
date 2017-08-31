package com.itc.zeas.project;

import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.exceptions.SqlIoException;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.ingestion.model.ZDPScheduler;
import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.usermanagement.model.UserLevelPermission;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.testrun.MRTestRunManager;
import com.itc.zeas.utility.testrun.MapRedParam;
import com.taphius.databridge.dao.IngestionLogDAO;
import com.zdp.dao.SearchCriteriaEnum;
import com.itc.zeas.project.model.SearchCriterion;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.model.OozieIngestionStatus;
import com.itc.zeas.model.ProcessedPipeline;
import com.itc.zeas.project.model.ProjectEntity;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.itc.zeas.utility.utils.CommonUtils;
import org.springframework.web.multipart.MultipartFile;

/**
 * 
 * @author 19217
 * 
 */

@RestController
@RequestMapping("/rest/service")
public class ProjectController {

	private static Logger LOG=Logger.getLogger(ProjectController.class);


	private ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
	private Long modelId;

	
	/**
	 * @param type
	 *            of entity
	 * @return list of entity
	 */

	@RequestMapping(value = "/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getEntity(@PathVariable("type") String type, HttpServletRequest httpServletRequest) {
		System.out.println("getEntity");
		EntityManager entityManager = new EntityManager();
		// List<Profile> profileList =
		// entityManager.getProfile("dataIngestion");
		// return profileList;
		List<Entity> entities = null;
		try {
			entities = entityManager.getEntity(type, httpServletRequest);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(entities);

	}

	/**
	 * Deletes a Project
	 * 
	 * @param projectId
	 *            id of project to be deleted
	 * @param httpRequest
	 *            httpRequest instance
	 * @return instance of ResponseEntity which includes result of delete
	 * @throws SQLException 
	 */
	@RequestMapping(value = "/projectmanagement/deleteProject/{projectId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody
	ResponseEntity<String> deleteProject(
			@PathVariable("projectId") Long projectId,
			HttpServletRequest httpRequest) throws SQLException {
		ProjectManager projectManager = new ProjectManager();
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		ResponseEntity<String> responseEntity = null;
		try {
			responseEntity = projectManager.deleteProject(
					userName, projectId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseEntity;
	}
	/**
	 * service responsible for providing Map Reduce test run functionality
	 *
	 * @param mapRedJarFile
	 *            jar file containing Mapper and Reducer class
	 * @return Success for successful map reduce execution Or Fail if map reduce
	 *         fails.
	 */
	@RequestMapping(value = "/uploadMapRedJar", method = RequestMethod.POST)
	public @ResponseBody String mapRedJarUpload(@RequestParam("file") MultipartFile mapRedJarFile) {
		LOG.debug("A request came to service mapRedJarUpload");
		MapRedParam mapRedParam = new MapRedParam();

		MRTestRunManager mrTestRunManager = new MRTestRunManager();
		String testResult = mrTestRunManager.uploadJar(mapRedJarFile, mapRedParam);
		return testResult;
	}
	/**
	 * provide status of Project and Module associated the project
	 *
	 * @param projectId
	 *            Id of project
	 * @param version
	 *            Project Version
	 * @return model object ProjectRunStatus which includes project and
	 *         associated module run status
	 * @throws SQLException
	 */
	@RequestMapping(value = "/projectrunstatus/{projectid}/{version}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<Object> getProjectRunStatus(@PathVariable("projectid") Long projectId,
													  @PathVariable("version") Integer version, HttpServletRequest httpRequest) throws SQLException {
		LOG.debug("inside function getProjectRunStatus");
		ProjectManager projectManager = new ProjectManager();
		ResponseEntity<Object> responseEntity = null;
		try {
			responseEntity = projectManager.getProjectRunStatus(projectId, version, httpRequest);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return responseEntity;
	}
	/**
	 * @return list of data paas frequencies which will be displayed as drop
	 *         down in UI
	 * @throws IOException
	 */
	@RequestMapping(value = "/list/{entityType}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> listDataSchema(@PathVariable("entityType") String entityType) throws IOException {
		EntityManager entityManager = new EntityManager();
		List<String> schemas = null;
		try {
			schemas = entityManager.listEntity(entityType);
		} 
		catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(schemas);

	}

	/**
	 * @param scheduler
	 * @return ResponseEntity
	 */

	/**
	 * @param projectId
	 * @return
	 */
	@RequestMapping(value = "/project/schedule/{projectId}", method = RequestMethod.GET, headers = "Accept=application/json,application/text")
	public ResponseEntity<ZDPScheduler> getSchedule(@PathVariable("projectId") Long projectId) {
		ZDPScheduler scheduler = dao.getScheduler(projectId);
		ResponseEntity<ZDPScheduler> responseEntity = new ResponseEntity<ZDPScheduler>(scheduler, HttpStatus.OK);
		return responseEntity;
	}


	@RequestMapping(value = "/project/schedule/{projectId}", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<String> saveSchedule(@RequestBody ZDPScheduler scheduler) {
		ResponseEntity<String> responseEntity = null;

		dao.persistScedulerDetail(scheduler);
		responseEntity = new ResponseEntity<String>("Scheduler Information Saved Succesfully.", HttpStatus.OK);
		return responseEntity;
	}

	/**
	 * This Rest Uri supports invoking project execution in back end
	 *
	 * @param projectId
	 *            {@link Integer} Project id param version {@link Integer}
	 *            Project version
	 * @return
	 * @return List<JSONObject> this call will invoke on run project click
	 */
	@RequestMapping(value = "/project/{projectId}/{version}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<String> getStagesfromPipeline(@PathVariable("projectId") Long projectId,
																	  @PathVariable("version") Integer version, HttpServletRequest httpRequest) {
		ResponseEntity<String> responseEntity = null;

		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		com.itc.zeas.utility.CommonUtils commonUtils = new com.itc.zeas.utility.CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		Boolean haveValidPermission = false;
		IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
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
			ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
			try {
				List<SearchCriterion> criterions = new ArrayList<>();
				SearchCriterion c1 = new SearchCriterion("id", projectId.toString(), SearchCriteriaEnum.EQUALS);
				SearchCriterion c2 = new SearchCriterion("version", version.toString(), SearchCriteriaEnum.EQUALS);
				criterions.add(c1);
				criterions.add(c2);
				ProjectEntity projectEntity = dao.findExactObject(ZDPDaoConstant.ZDP_PROJECT_TABLE, criterions);
				String pipelineName = projectEntity.getId() + "-" + projectEntity.getVersion();
				pipelineNotification(pipelineName);
				String opString = "Project : " + pipelineName + " ready to start";
				responseEntity = new ResponseEntity<String>(opString, HttpStatus.OK);
				ZDPDataAccessObject accessObjectImpl = new ZDPDataAccessObjectImpl();
				List<String> users = new ArrayList<>();
				users.add(userName);
				accessObjectImpl.addActivitiesBatchForNewAPI(projectEntity.getName(),
						"Project '" + projectEntity.getName() + "' initiated by " + userName, ZDPDaoConstant.PROJECT_ACTIVITY,
						ZDPDaoConstant.INITIATE_ACTIVITY, users, userName);

				LOG.info("User " + userName + ": Started project '" + projectEntity.getName() + "' .");
				java.util.Date dt = new java.util.Date();
				java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				String currentTime = sdf.format(dt);
				Timestamp timestamp = Timestamp.valueOf(currentTime);
				ingestionLogDAO.addGraylogInfo(timestamp, userName, "project", projectEntity.getName(), "Started");
			} catch (Exception e) {
				e.printStackTrace();
				responseEntity = new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
			}

		} else {
			// don't have valid permission to execute the project
			LOG.info("don't have enough permission to execute project with id: " + projectId);
			responseEntity = new ResponseEntity<String>("don't have enough permission to execute the project",
					HttpStatus.FORBIDDEN);
		}

		return responseEntity;
	}
	/**
	 * This method is used to get the completed processed pipelines
	 *
	 * @return List<JSONObject>
	 * @throws IOException
	 */
	@RequestMapping(value = "/pipeline/getPipelines", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody List<ProcessedPipeline> getProcessedPipelines() throws IOException {
		EntityManager entityMngr = new EntityManager();
		List<ProcessedPipeline> processedPipelineList = new ArrayList<ProcessedPipeline>();
		try {
			processedPipelineList = entityMngr.getProcessedPipelines();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return processedPipelineList;

	}



	/**
	 * This api is used for project export(in json) at given directory path
	 * location(e.g path/project_id_version.json).
	 *
	 * @param projectEntity
	 * @return
	 */
	@RequestMapping(value = "/projectExport", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody String projectExport(@RequestBody ProjectEntity projectEntity) {

		System.out.println("project name id:" + projectEntity.getId() + ":" + projectEntity.getName() + ":" + projectEntity.getVersion());
		System.out.println("project exportedpath :" + projectEntity.getExportLocation());
		ZDPDataAccessObjectImpl accessObject = new ZDPDataAccessObjectImpl();
		String isExported = "project export failed";
		try {
			isExported = accessObject.exportProject(projectEntity, projectEntity.getExportLocation());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isExported;
	}
	

	// get latest version object
	@RequestMapping(value = "/listObject/{schemaType}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getObject(@PathVariable("schemaType") String type, HttpServletRequest request) {
		System.out.println("getEntity");
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(request);

		List<ProjectEntity> entities = null;
		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		try {

			if (type.equalsIgnoreCase("project")) {
				entities = dao.findLatestVersionProjects(type);

				// TODO get project from all group
				ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
				Boolean isSuperUser = zdpUserAccess.isSuperUser(userName);
				if (isSuperUser) {
					LOG.debug("user is  super user");
					for (ProjectEntity projectEntity : entities) {
						projectEntity.setPermissionLevel(UserManagementConstant.READ_WRITE_EXECUTE);
					}
				} else {
					Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userName);
					Iterator<ProjectEntity> iterator = entities.iterator();
					// for (com.zdp.dao.Entity entity : entities) {
					while (iterator.hasNext()) {
						ProjectEntity projectEntity = (ProjectEntity) iterator.next();
						String createdBy = projectEntity.getCreatedBy();

						UserLevelPermission userLevelPermission = zdpUserAccess.getUserLevelPermission(userName);
						int userLevelProjectPermission = userLevelPermission.getProjectPermission();
						if (createdBy.equals(userName)) {
							// creator is requesting user
							projectEntity.setPermissionLevel(userLevelProjectPermission);
						} else {
							Integer groupLevelpermission = userNamePermissionMap.get(createdBy);
							if (groupLevelpermission != null) {

								projectEntity.setPermissionLevel(groupLevelpermission & userLevelProjectPermission);
							} else {
								iterator.remove();
							}
						}
					}
				}

			} else {

				// TODO ask tuntun and put permission check it's related to
				// others module
				entities = dao.findObjects(type, null);
			}
		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(entities);

	}


	@RequestMapping(value = "/project/schema/{projectId}", method = RequestMethod.GET, produces = "application/json", headers = "Accept=application/json")
	public ResponseEntity<List<String>> getSchema(@PathVariable("projectId") Long projectId) {

		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		List<String> schemaList = getSchemaFromJSON(dao, projectId);

		return ResponseEntity.ok(schemaList);
	}

	@RequestMapping(value = "/project/outputpath/{projectId}", method = RequestMethod.GET, produces = "application/json", headers = "Accept=application/json")
	public ResponseEntity<Map<String, String>> getOutPutPath(@PathVariable("projectId") Long projectId) {

		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		Map<String, String> details = null;

		getSchemaFromJSON(dao, projectId);
		try {
			details = dao.getProjectOuputDetails(modelId);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ResponseEntity.ok(details);
	}

	/**
	 * 1. get Schema by Quering select id ,properties from zeas.module where
	 * project_id='1745' and component_type NOT IN('internal dataset') ORDER BY
	 * created DESC Limit 1;
	 *
	 * 2. get output location by Quering SELECT details FROM zeas.module_history
	 * where module_id='1752' ORDER BY end_time DESC Limit 1;
	 */

	@RequestMapping(value = "/project/chartData/{projectId}", method = RequestMethod.GET, produces = "application/json", headers = "Accept=application/json")
	public ResponseEntity<String> getChartData(@PathVariable("projectId") Long projectId) {

		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		List<JSONObject> arrayList = new ArrayList<JSONObject>();
		String outPutLocation = null;
		String outPutType = null;

		List<String> schemaList = getSchemaFromJSON(dao, projectId);

		try {
			Map<String, String> details = dao.getProjectOuputDetails(modelId);
			for (Map.Entry<String, String> entry : details.entrySet()) {
				outPutType = entry.getKey();
				outPutLocation = entry.getValue();
				System.out.println("outPutType" + outPutType + ": outPutLocation" + outPutLocation);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}


		LOG.info("Final json data is: " + arrayList.toString());
		return ResponseEntity.ok(arrayList.toString());
	}

	/**
	 * This api is used for project import using given json file.
	 *
	 * @param projectEntity
	 * @return
	 */
	@RequestMapping(value = "/projectImport", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody String projectImport(@RequestBody ProjectEntity projectEntity, HttpServletRequest httpRequest) {

		System.out.println("file input path:" + projectEntity.getLocation());
		ZDPDataAccessObjectImpl accessObject = new ZDPDataAccessObjectImpl();
		String imported = "import failed";
		try {
			com.itc.zeas.utility.CommonUtils commonUtils = new com.itc.zeas.utility.CommonUtils();
			String userName = commonUtils.extractUserNameFromRequest(httpRequest);
			imported = accessObject.importProject(projectEntity.getLocation(), userName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return imported;
	}

	@RequestMapping(value = "/validateProjectName/{projectName}", method = RequestMethod.GET, headers = "Accept=application/json")
	public String validateProjectName(@PathVariable String projectName, HttpServletRequest httpRequest) {

		LOG.debug("validateProjectName api \n projectName =" + projectName);
		com.itc.zeas.utility.CommonUtils commonUtils = new com.itc.zeas.utility.CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		// it is used to say project already exist with this name if value is
		// true.
		Boolean isProjectExist = true;
		EntityManager em = new EntityManager();
		isProjectExist = em.isProjectExist(projectName, userName);
		return isProjectExist.toString();
	}

















	private List<String> getSchemaFromJSON(ZDPDataAccessObjectImpl dao, Long projectId) {

		Map<Long, String> schemaMap = null;

		List<String> schemaList = new LinkedList<>();

		try {
			schemaMap = dao.getProjectSchema(projectId);
			String properties = null;
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




/*
	 * Create a notification entry for Pipeline start
	 */

	public void pipelineNotification(String pipelineName) {
		// Load and read existing properties file
		LOG.info("Going to start pipeline--" + pipelineName);
		System.out.println("Inside START write method =================");
		OutputStream output = null;
		try {
			File conf = new File(System.getProperty("user.home") + "/zeas/Config/notify");
			FileInputStream templateFile = new FileInputStream(conf);
			Properties prop = new Properties();
			prop.load(templateFile);
			templateFile.close();
			prop.setProperty(pipelineName, "START");
			output = new FileOutputStream(conf, false);
			// save properties to conf_root folder
			prop.store(output, null);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					LOG.error("Error saving Notify file -" + e.getMessage());
				}
			}
		}
	}

	/**
	 * this method gets the ingestion scheduler status from oozie it gets name
	 * of project and its id and based on that it returns status json object
	 *
	 * @return
	 */

	@RequestMapping(value = "/projectSchedulerHistory/{projectName}/{proId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody String getProjectIngestionStatus(@PathVariable("projectName") String projectName,
														  @PathVariable("proId") Integer projectId, HttpServletRequest httpRequest) {
		OozieIngestionStatus status = new OozieIngestionStatus();
		return status.getStatusJson(projectName, projectId);
	}

}
