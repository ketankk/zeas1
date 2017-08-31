package com.itc.zeas.profile;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.itc.zeas.exceptions.PermissionException;
import com.itc.zeas.exceptions.SqlIoException;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.model.DatasetPathDetails;
import com.itc.zeas.model.ModuleSchema;
import com.itc.zeas.model.PipelineJob;
import com.itc.zeas.model.ProjectRunHistory;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.project.PipelineJobInfoController;
import com.itc.zeas.project.extras.QueryConstants;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.project.model.NameAndDataType;
import com.itc.zeas.project.model.ProjectEntity;
import com.itc.zeas.project.model.SearchCriterion;
import com.itc.zeas.usermanagement.model.UserLevelPermission;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.CommonResourceLoader;
import com.itc.zeas.utility.ExportControllerUtility;
import com.itc.zeas.utility.FileUtility;
import com.itc.zeas.utility.filereader.ComparedResultsReader;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.utility.filereader.ValidatorErrorInfo;
import com.itc.zeas.utility.utils.CommonUtils;
import com.zdp.dao.SearchCriteriaEnum;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.zdp.dao.ZDPDataLineage;

/**
 * @author 16765
 */
@RestController
@RequestMapping("/rest/service")
public class EntityController {

	private static final Logger LOG = Logger.getLogger(EntityController.class);
	// @Resource(name = "entityManager")
	EntityManager entityManager;

	@RequestMapping(value = "/addObject", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<?> addObject(@RequestBody ProjectEntity projectEntity, HttpServletRequest request)
			throws ZeasSQLException, SQLException {

		ProjectEntity responseProjectEntity = null;
		ProjectEntity workspaceProjectEntity = null;
		String userName = "";
		CommonUtils commonUtils = new CommonUtils();
		String accessToken = commonUtils.extractAuthTokenFromRequest(request);
		userName = commonUtils.getUserNameFromToken(accessToken);
		List<String> users = new ArrayList<>();
		users.add(userName);
		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();

		String schemaType = projectEntity.getSchemaType();
		if (schemaType.equalsIgnoreCase("project")) {
			String workspaceName = projectEntity.getWorkspace_name();
			String[] strArr = workspaceName.split("\\|");
			if ("Add Workspace".equalsIgnoreCase(strArr[0].trim())) {
				ProjectEntity tempProjectEntity = new ProjectEntity();
				tempProjectEntity.setName(strArr[1]);
				tempProjectEntity.setCreatedBy(projectEntity.getCreatedBy());
				tempProjectEntity.setSchemaType(ZDPDaoConstant.ZDP_WORKSPACE);
				workspaceProjectEntity = dao.addEntity(tempProjectEntity);
				if (workspaceProjectEntity != null) {
					projectEntity.setWorkspace_name(strArr[1]);
					responseProjectEntity = dao.addEntity(projectEntity);
				}
			} else {
				responseProjectEntity = dao.addEntity(projectEntity);

			}
			int version = 0;
			if (responseProjectEntity.getVersion() != null) {
				version = Integer.parseInt(responseProjectEntity.getVersion());
			}

			if (version == 1) {
				accessObjectImpl.addActivitiesBatchForNewAPI(projectEntity.getName(),
						"New project '" + projectEntity.getName() + "' created by " + userName,
						ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.CREATE_ACTIVITY, users, userName);
				LOG.info("User " + userName + ": New project '" + projectEntity.getName() + "' created successfully.");
			} else if (version > 1) {
				accessObjectImpl.addActivitiesBatchForNewAPI(projectEntity.getName(),
						"Project '" + projectEntity.getName() + "' updated by " + userName,
						ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.UPDATE_ACTIVITY, users, userName);
				LOG.info("User " + userName + ": Project '" + projectEntity.getName() + "' updated successfully.");
			}
		} else {
			responseProjectEntity = dao.addEntity(projectEntity);
			if (schemaType.equalsIgnoreCase("module")) {
				LOG.info("User " + userName + ": Project '" + projectEntity.getName() + "' updated successfully.");

			}
		}
		return ResponseEntity.ok(responseProjectEntity);
	}

	/**
	 * List of UI attributes to be displayed for particular component.
	 * 
	 * @param container
	 *            Name of container
	 * @param name
	 *            Particular component for given container.
	 * @return {@link List} of entries to be displayed.
	 * @throws IOException
	 */
	@RequestMapping(value = "/list/{container}/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> listConfigurations(@PathVariable("container") String container,
			@PathVariable("name") String name) throws IOException {
		EntityManager entityManager = new EntityManager();
		List<String> schemas = null;
		try {
			schemas = entityManager.listConfigurations(container, name);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

		return ResponseEntity.ok(schemas);
	}

	@RequestMapping(value = "/deleteObject", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Boolean deleteObject(@RequestBody ProjectEntity projectEntity) throws ZeasSQLException {

		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		List<SearchCriterion> criterions = new ArrayList<>();
		Long id = projectEntity.getId();
		Integer version = Integer.valueOf(projectEntity.getVersion());
		SearchCriterion c1 = new SearchCriterion("id", id.toString(), SearchCriteriaEnum.EQUALS);
		SearchCriterion c2 = new SearchCriterion("version", version.toString(), SearchCriteriaEnum.EQUALS,
				QueryConstants.QUERY_AND_TYPE);
		criterions.add(c1);
		criterions.add(c2);
		Boolean isDeleted = false;
		;
		try {
			isDeleted = dao.deleteEntity(projectEntity.getSchemaType(), criterions);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isDeleted;
	}

	/**
	 * @return Entity
	 * @throws ZeasSQLException
	 * @throws IOException
	 */
	@RequestMapping(value = "/addStage", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Entity addStage(@RequestBody Entity entity, HttpServletRequest request) {
		// TODO why hive??
		EntityManager entityManager = new EntityManager();
		entity.setType("Hive");
		try {
			entityManager.addEntity(entity, request);
			return entityManager.getEntityByName(entity.getName());
		} catch (Exception e) {
			LOG.error("Exception while adding new entity " + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * this method is to edit/update data schema
	 * <p>
	 * type, and entityId
	 *
	 * @return Entity
	 * @throws IOException
	 */
	@RequestMapping(value = "/{type}/{entityId}", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> updateEntity(@RequestBody Entity entity, @PathVariable("type") String type,
			@PathVariable("entityId") Long entityId, HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws IOException {
		EntityManager entityMngr = new EntityManager();
		try {
			entityMngr.updateEntity(entity, type, entityId, httpServletRequest, httpServletResponse);
		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(entity);
	}

	/**
	 * this method is to return list of entity for given entityId
	 *
	 * @param type
	 *            , entityId
	 * @return Entity
	 * @throws IOException
	 */
	@RequestMapping(value = "/{type}/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getEntityById(@PathVariable("type") String type,
			@PathVariable("entityId") Long entityId, HttpServletRequest httpServletRequest) throws IOException {

		EntityManager entityManager = new EntityManager();
		Entity entity = null;
		try {
			entity = entityManager.getEntityById(type, entityId, httpServletRequest);
		}  catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(entity);
	}

	@RequestMapping(value = "/update/{type}/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody Entity getEntityByIdWhileUpdate(@PathVariable("type") String type,
			@PathVariable("entityId") Long entityId, HttpServletRequest httpServletRequest,
			HttpServletResponse response) throws IOException {
		//
		Entity entity = new Entity();
		// added by deepak for authorization check starts

		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
		ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
		Boolean haveValidPermission = null;
		try {
			haveValidPermission = zdpUserAccess.validateUserPermissionForResource(
					UserManagementConstant.ResourceType.DATASET, userName, entityId, UserManagementConstant.READ_WRITE);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (!haveValidPermission) {
			String errorMessage = "Failed!  Don't have enough permission to update.";
			try {
				response.getWriter().print(errorMessage);
				response.setStatus(403);
			} catch (IOException e) {
				e.printStackTrace();
				response.setStatus(500);
			}
		} else {
			EntityManager entityManager = new EntityManager();
			try {
				entity = entityManager.getEntityById(type, entityId, httpServletRequest);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return entity;
	}

	@RequestMapping(value = "/listObject/{schemaType}/{id}/{version}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<Object> getObjectById(@PathVariable("schemaType") String type,
			@PathVariable("id") Long entityId, @PathVariable("version") Integer version,
			HttpServletRequest httpRequest) {
		ResponseEntity<Object> responseEntity = null;
		ProjectEntity projectEntity = null;
		// added by Deepak starts for verifying authorization
		// Boolean haveValidPermission = true;
		Integer maxPermissionForReqResource = null;
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		UserManagementConstant.ResourceType resourceType = null;
		if (type.equals("project")) {
			resourceType = UserManagementConstant.ResourceType.PROJECT;
		} else if (type.equals("module")) {
			resourceType = UserManagementConstant.ResourceType.MODULE;
		}
		if (resourceType != null) {

			// /
			try {
				maxPermissionForReqResource = zdpUserAccessImpl.validateAndReturnMaxPermission(resourceType, userName,
						entityId, UserManagementConstant.READ);
				ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
				try {
					List<SearchCriterion> criterions = new ArrayList<>();
					SearchCriterion c1 = new SearchCriterion("id", entityId.toString(), SearchCriteriaEnum.EQUALS);
					SearchCriterion c2 = new SearchCriterion("version", version.toString(), SearchCriteriaEnum.EQUALS);
					criterions.add(c1);
					criterions.add(c2);
					projectEntity = dao.findExactObject(type, criterions);
					if (resourceType != null) {
						projectEntity.setPermissionLevel(maxPermissionForReqResource);
					}
					responseEntity = new ResponseEntity<Object>(projectEntity, HttpStatus.OK);
				} catch (Exception e) {
					responseEntity = new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
					e.printStackTrace();
				}
			} catch (PermissionException.NotHaveRequestedPermissionException e) {
				LOG.info("don't have enough permission to read resource of type: " + resourceType + " with id: "
						+ entityId);
				responseEntity = new ResponseEntity<Object>("don't have enough permission to read the resource",
						HttpStatus.FORBIDDEN);
			} catch (Exception e) {
				responseEntity = new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
				e.printStackTrace();
			}
		}

		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();

		Map<Long, String> schemaMap = null;

		// JSONObject json = new JSONObject();json.getJSONObject(key)
		Long modelId = null;
		String properties = null;

		try {
			schemaMap = dao.getProjectSchema(entityId);

			for (Map.Entry<Long, String> entry : schemaMap.entrySet()) {
				modelId = entry.getKey();
				properties = entry.getValue();
				System.out.println(entry.getKey() + "/" + entry.getValue());
			}
			System.out.println("data :" + schemaMap);

			if (properties != null) {

			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return responseEntity;
	}

	/**
	 * this method is to delete an entity
	 *
	 * @param entityId
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "/{entityId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody void deleteEntity(@PathVariable("entityId") Integer entityId) throws IOException {
		EntityManager entityManager = new EntityManager();
		try {
			entityManager.deleteEntity(entityId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * List of UI attributes to be displayed for particular component.
	 *
	 * @param container
	 *            Name of container
	 * @param name
	 *            Particular component for given container.
	 * @return {@link List} of entries to be displayed.
	 * @throws IOException
	 */

	/**
	 * @return list of data paas frequencies which will be displayed as drop
	 *         down in UI
	 */
	@RequestMapping(value = "/listFrequency", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listFrequency() {
		List<String> frequencies = CommonResourceLoader.frequencyLoader();
		return frequencies;
	}

	/**
	 * @return list of schema types which will be displayed as drop down in UI
	 */
	@RequestMapping(value = "/listSchemaType", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listSchemaType() {
		List<String> schemaTypes = CommonResourceLoader.schemaTypeLoader();
		return schemaTypes;
	}

	@RequestMapping(value = "/getMetaSchema", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody List<List<String>> getMetaSchema(@RequestParam("file") MultipartFile schemaFile,
			@RequestParam("fileName") String fileName, @RequestParam("fileType") String fileType,
			@RequestParam("format") String format, @RequestParam("mFlag") String mFlag,
			@RequestParam("hFlag") String hFlag, @RequestParam("noOfColumn") String noOfColumn,
			@RequestParam("fixedValues") String fixedValues, @RequestParam("rowDeli") String rowDeli,
			@RequestParam("colDeli") String colDeli) {

		ExtendedDetails sObject = new ExtendedDetails();
		sObject.setFileName(fileName);
		sObject.setFileType(fileType);
		sObject.setFileType(format);
		sObject.setmFlag(mFlag);
		sObject.sethFlag(hFlag);
		if (!("".equalsIgnoreCase(noOfColumn) || noOfColumn == null)) {
			sObject.setNoOfColumn(Integer.parseInt(noOfColumn));
		} else {
			sObject.setNoOfColumn(0);
		}
		sObject.setFixedValues(fixedValues);
		rowDeli = rowDeli.replaceAll("[^\\p{Alpha}]+", "");
		colDeli = colDeli.replaceAll("[^\\p{Alpha}]+", "");
		sObject.setRowDeli(rowDeli);
		sObject.setColDeli(colDeli);
		String fName = sObject.getFileName();
		List<String> headerList = new ArrayList<>();
		List<String> dataTypeList = new ArrayList<>();
		if (fName == null || fName.isEmpty()) {
			fName = FileReaderConstant.RDBMS_TYPE;
		}

		if (sObject.getmFlag().equalsIgnoreCase("true")) {
			File convFile = new File(schemaFile.getOriginalFilename());
			try {
				schemaFile.transferTo(convFile);
			} catch (IllegalStateException | IOException e) {
				e.printStackTrace();
			}
			// Todo check this also
			/*
			 * for (NameAndDataType nameAndDataType :
			 * StreamDriverManager.getSchema(convFile)) {
			 * headerList.add(nameAndDataType.getName());
			 * dataTypeList.add(nameAndDataType.getDataType()); }
			 */
		}
		SampleDataView dataView = new SampleDataView(fName, sObject, headerList, dataTypeList);
		List<List<String>> sampleData = new ArrayList<>();
		try {
			sampleData = dataView.getSampleData();
			System.out.println(sampleData);
			return sampleData;
		} catch (ZeasException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			// response.setStatus(e.getErrorCode());
			/*
			 * try { //response.getWriter().print(e.toString()); } catch
			 * (IOException e1) { // TODO Auto-generated catch block
			 * e1.printStackTrace(); }
			 */
			return null;

		}

	}

	@RequestMapping(value = "/getSchemaAuto", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getSchemaAuto(@RequestBody ExtendedDetails sObject) {

		String fName = sObject.getFileName();
		if (fName == null || fName.isEmpty()) {
			fName = FileReaderConstant.RDBMS_TYPE;
		}
		List<String> headerList = null;
		List<String> dataTypeList = null;
		if (sObject.getmFlag() == null) {
			sObject.setmFlag("false");
		}
		if (sObject.gethFlag() == null) {
			sObject.sethFlag("false");
		}
		SampleDataView dataView = new SampleDataView(fName, sObject, headerList, dataTypeList);
		List<List<String>> sampleData = new ArrayList<>();
		try {
			sampleData = dataView.getSampleData();
			return ResponseEntity.ok(sampleData);
		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
			return null;
		}

	}

	/**
	 * This function will provide oozie job information such as job id,create
	 * time, start time,end time,current status for given pipeline job id
	 *
	 * @param pipelineJobId
	 *            id of pipeline job whose job information needs to be provided
	 * @return an instance of PipelineJob
	 * @throws SQLException
	 * @author 19217
	 */
	@RequestMapping(value = "/PipelineJobInfo/{jobId}", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody PipelineJob getPipelineJobInfo(@PathVariable String pipelineJobId) throws SQLException {
		PipelineJob PipelineJob = null;
		PipelineJob = PipelineJobInfoController.getPipelieJobInfo(pipelineJobId);
		return PipelineJob;
	}

	/**
	 * this method helps to validate the validator for corresponding column
	 * <p>
	 * type, and entityId
	 *
	 * @return List<String>
	 */
	@RequestMapping(value = "/validatorValidation", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> validateValidator(@RequestBody Entity entity) {

		ValidatorErrorInfo errorInfo = new ValidatorErrorInfo();
		// return errorInfo.getErrors(entity.getJsonblob());
		return ResponseEntity.ok(errorInfo.getErrors(entity.getJsonblob()));

	}

	/**
	 * Verifying user access and creating target directories if not exists
	 *
	 * @param entity
	 * @return
	 */
	@RequestMapping(value = "/verifyTargetHDFSPathAcess", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Boolean verifyTargetHDFSPathAccess(@RequestBody Entity entity, HttpServletResponse response) {

		return true;
	}

	/**
	 * @param entity
	 * @param request
	 * @return
	 * @throws ZeasSQLException
	 */
	@RequestMapping(value = "/exportHiveView", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Boolean exportHiveView(@RequestBody Entity entity, HttpServletRequest request,
			HttpServletResponse response, @RequestParam(defaultValue = "csv") String format) throws ZeasSQLException {
		boolean status = new ExportControllerUtility().isFileExported(entity, format);
		return status;
	}

	@RequestMapping(value = "/getColumnsById/{datasetId}/{version}/{table}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getColumnsForDatasetTable(@PathVariable String datasetId, @PathVariable String version,
			@PathVariable String table) {
		LOG.info("datasetid =" + datasetId + " version=" + version + " table=" + table);
		List<ModuleSchema> columns = null;
		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		try {
			// columns =
			// em.getColumns(em.getSchemaName(Integer.parseInt(datasetId)));
			columns = dao.getColumnAndDatatype(datasetId);
			if ((columns != null && columns.size() == 0) || columns == null) {

				columns = dao.getColumnNames("module", datasetId, version);

			}
			if (columns != null && columns.size() > 0) {
				ModuleSchema tableEntry = new ModuleSchema(table, null);
				columns.add(0, tableEntry);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(columns);

	}

	@RequestMapping(value = "/getProjectRunHistory/{name}/{id}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<Object> getProjectRunHistory(@PathVariable String name, @PathVariable String id,
			HttpServletRequest httpRequest) {
		ResponseEntity<Object> responseEntity = null;
		// added by Deepak starts for verifying authorization
		Boolean haveValidPermission = true;
		Long projectId = Long.parseLong(id);
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpRequest);
		ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
		try {
			haveValidPermission = zdpUserAccessImpl.validateUserPermissionForResource(
					UserManagementConstant.ResourceType.PROJECT, userName, projectId, UserManagementConstant.READ);
		} catch (SqlIoException.IoException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (SqlIoException.SqlException exception) {
			new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * catch (InvalidArgumentException.InvalidResTypeException
		 * invalidResTypeException) { new ResponseEntity<Object>(
		 * "resource type is not valid", HttpStatus.UNPROCESSABLE_ENTITY); }
		 */
		if (haveValidPermission) {
			// added by Deepak Ends
			LOG.debug("datasetid =" + name + " version=" + id);
			List<ProjectRunHistory> projectRunHistory = new ArrayList<>();
			ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
			try {
				projectRunHistory = dao.getProjectRunHistoryInfo(name, id);
				responseEntity = new ResponseEntity<Object>(projectRunHistory, HttpStatus.OK);
			} catch (SQLException e) {
				LOG.error(e.getMessage());
				responseEntity = new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
			}
			LOG.debug(projectRunHistory);
		} else {
			LOG.info("don't have enough permission to read resource of type project with id: " + id);
			responseEntity = new ResponseEntity<Object>(
					"don't have enough permission to read the run history of resource", HttpStatus.FORBIDDEN);
		}
		return responseEntity;
	}

	/**
	 * @return
	 * @throws SQLException
	 */
	@RequestMapping(value = "/getDetailsForSelectedTables", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getDetailsForSelectedTables(@RequestBody ExtendedDetails sObject)
			throws SQLException {
		RdmsMultipleTablesReader rmtr = new RdmsMultipleTablesReader();
		Map<String, List<NameAndDataType>> res = null;
		try {
			res = rmtr.getColumnDetailsForSelectedTables(sObject);

		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(res);

	}

	@RequestMapping(value = "/getModuleHistory/{p_id}/{id}/{version}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ProjectEntity getModuleHistory(@PathVariable String p_id, @PathVariable String id,
			@PathVariable String version, HttpServletRequest httpRequest) {

		LOG.debug("moduleid =" + id + " version=" + version);
		// List<ZDPModuleHistory> projectRunHistory = new ArrayList<>();
		ProjectEntity en = new ProjectEntity();
		try {
			List<SearchCriterion> list = new ArrayList<>();
			SearchCriterion criterion = new SearchCriterion("module_id", id, SearchCriteriaEnum.EQUALS);
			SearchCriterion criterion1 = new SearchCriterion("project_run_id", p_id, SearchCriteriaEnum.EQUALS);
			SearchCriterion criterion2 = new SearchCriterion("version", version, SearchCriteriaEnum.EQUALS);
			list.add(criterion);
			list.add(criterion1);
			list.add(criterion2);
			ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
			en = dao.findExactObject("module_history", list);

		} catch (Exception e) {
			LOG.error(e.getMessage());

		}
		// LOGGER.debug(projectRunHistory);

		return en;
	}

	@RequestMapping(value = "/getUsedProjectList/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> getUsedProjectList(@PathVariable String name) throws Exception {
		ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();

		return accessObjectImpl.getUsedProjectList(name + "_dataset");
	}

	@RequestMapping(value = "/getJsonForGraph/{projectName}/{dataSetName}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ProjectEntity getJsonForGraph(@PathVariable String projectName, @PathVariable String dataSetName)
			throws Exception {

		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		List<ProjectEntity> projectEntityList = dao.findLatestVersionProjects("project");
		ZDPDataLineage dataLineage = new ZDPDataLineage();

		return dataLineage.getJson(projectName, projectEntityList, dataSetName + "_dataSet");

	}

	/**
	 * This REST API is used to define output schema for any Transformations.
	 * Currently it is used to define schema for MR and Pig Actions. User loads
	 * a schema file, which will be parsed to return list of output columns.
	 *
	 * @param schemaFile
	 *            Name of the schema file uploaded
	 * @return List of output columns {@link List} of {@link NameAndDataType}
	 */
	@RequestMapping(value = "/uploadSchema", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody List<NameAndDataType> uploadSchema(@RequestParam("file") MultipartFile schemaFile) {
		try {
			File convFile = new File(schemaFile.getOriginalFilename());
			schemaFile.transferTo(convFile);
			// TODO change this
			return null;// StreamDriverManager.getSchema(convFile);

		} catch (IllegalStateException | IOException e) {
			e.printStackTrace();
		}
		return new ArrayList<>();
	}

	// find the log details for databridge or ZDP Master
	@RequestMapping(value = "/getLogDetails/{type}/{name}/{version}/{count}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getLogDetails(@PathVariable String type, @PathVariable String name,
			@PathVariable String version, @PathVariable Integer count, HttpServletRequest httpServletRequest)
			throws Exception {

		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
		List<String> logs = FileUtility.getLogs(type, name, version, userName, count);
		Integer noOfRecords = count + logs.size();
		logs.add(0, noOfRecords.toString());
		return ResponseEntity.ok(logs);
	}

	@RequestMapping(value = "/verifyMultiTableQueries", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody void verifyMultiTableQueries(@RequestBody ExtendedDetails sObject,
			HttpServletResponse response) throws ZeasException {
		RdmsMultipleTablesReader rmtr = new RdmsMultipleTablesReader();

		try {
			System.out.println("Verifying multitable queries");
			List<String> failedQueryList = rmtr.verifyQueries(sObject);

			if (failedQueryList != null && failedQueryList.size() > 0) {

				String responseMsg = "Verify query for ";
				for (int i = 0; i < failedQueryList.size(); i++) {
					String tableName = failedQueryList.get(i);

					if (i != (failedQueryList.size() - 1)) {
						responseMsg += tableName + ", ";
					} else {
						responseMsg += tableName;
					}
				}
				response.getWriter().print(responseMsg);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	/**
	 * details
	 *
	 * @return Map<String, List<String>>
	 * @throws SQLException
	 */
	@RequestMapping(value = "/getComparisonResult", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Map<String, List<List<String>>> getComparisonResult(@RequestBody String details,
			HttpServletResponse response) {
		Map<String, List<List<String>>> results = null;
		try {
			ComparedResultsReader crr = new ComparedResultsReader();
			results = crr.getResults(details);
		} catch (ZeasException e) {
			response.setStatus(e.getErrorCode());
			try {
				response.getWriter().print(e.toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}
		return results;

	}
	
	@RequestMapping(value = "/getDatasetPathDetails", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody DatasetPathDetails getDatasetPathDetails() {
		EntityManager manager = new EntityManager();
		return manager.getDatasetPathDetails();
	}

	@RequestMapping(value = "/project/projectType/{projectId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public String restoreArchive(@PathVariable String projectId) {
		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		return dao.getProjectTypeDetail(Integer.parseInt(projectId));
	}

}
