package com.itc.taphius.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.model.Entity;
import com.itc.taphius.model.PipelineJob;
import com.itc.taphius.model.PipelineStageLog;
import com.itc.taphius.model.Profile;
import com.itc.taphius.utility.CommonResourceLoader;
import com.itc.taphius.utility.ConfigurationReader;
import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasSQLException;
import com.itc.zeas.filereader.ExtendedDetails;
import com.itc.zeas.filereader.FileReaderConstant;
import com.itc.zeas.filereader.SampleDataView;
import com.itc.zeas.filereader.ValidatorErrorInfo;
import com.itc.zeas.testrun.MRTestRunManager;
import com.itc.zeas.testrun.MapRedParam;
import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.validation.mr.IngestionLogDetails;

/**
 * @author 16765
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class EntityController {

	private Logger logger = Logger.getLogger(EntityController.class);

	/**
	 * This service will provide list of Profile , every Profile is
	 * representation of consolidated information about data source, data
	 * set,schema and scheduler
	 * 
	 * @return list of type Profile
	 */
	@RequestMapping(value = "/profiles", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<Profile> getProfile() {
		EntityManager entityManager = new EntityManager();
		List<Profile> profileList = entityManager.getProfiles();
		return profileList;
	}

	/**
	 * @param type
	 *            of entity
	 * @return list of entity
	 */

	@RequestMapping(value = "/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<Entity> getEntity(@PathVariable("type") String type) {
		System.out.println("getEntity");
		EntityManager entityManager = new EntityManager();
		// List<Profile> profileList =
		// entityManager.getProfile("dataIngestion");
		// return profileList;
		List<Entity> entities = entityManager.getEntity(type);
		return entities;

	}

	/**
	 * @param Entity
	 * @return Entity
	 * @throws ZeasSQLException 
	 */
	@RequestMapping(value = "/addEntity", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Entity addEntity(@RequestBody Entity entity) throws ZeasSQLException {
		EntityManager entityManager = new EntityManager();

		entityManager.addEntity(entity);
		entity = entityManager.getEntityByName(entity.getName());
		return entity;

	}

	/**
	 * @param Entity
	 * @return Entity
	 * @throws ZeasSQLException 
	 */
	@RequestMapping(value = "/addStage", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Entity addStage(@RequestBody Entity entity) throws ZeasSQLException {
		EntityManager entityManager = new EntityManager();
		entity.setType("Hive");
		entityManager.addEntity(entity);
		return entityManager.getEntityByName(entity.getName());

	}

	/**
	 * this method is to edit/update data schema
	 * 
	 * @param String
	 *            type, and entityId
	 * @return Entity
	 */
	@RequestMapping(value = "/{type}/{entityId}", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Entity updateEntity(@RequestBody Entity entity,
			@PathVariable("type") String type,
			@PathVariable("entityId") Integer entityId) {
		EntityManager entityMngr = new EntityManager();
		entityMngr.updateEntity(entity, type, entityId);
		return entity;

	}

	/**
	 * this method is to return list of entity for given entityId
	 * 
	 * @param type
	 *            , entityId
	 * @return Entity
	 */
	@RequestMapping(value = "/{type}/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody
	Entity getEntityById(@PathVariable("type") String type,
			@PathVariable("entityId") Integer entityId) {

		EntityManager entityManager = new EntityManager();
		Entity entity = entityManager.getEntityById(type, entityId);
		return entity;

	}

	/**
	 * this method is to delete an entity
	 * 
	 * @param entityId
	 * @return
	 */
	@RequestMapping(value = "/{entityId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody
	void deleteEntity(@PathVariable("entityId") Integer entityId) {
		EntityManager entityManager = new EntityManager();
		entityManager.deleteEntity(entityId);
	}

	/**
	 * 
	 * @return list of data paas frequencies which will be displayed as drop
	 *         down in UI
	 */
	@RequestMapping(value = "/list/{entityType}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listDataSchema(
			@PathVariable("entityType") String entityType) {
		EntityManager entityManager = new EntityManager();
		List<String> schemas = entityManager.listEntity(entityType);
		return schemas;
	}

	/**
	 * List of UI attributes to be displayed for particular component.
	 * 
	 * @param container
	 *            Name of container
	 * @param name
	 *            Particular component for given container.
	 * @return {@link List} of entries to be displayed.
	 */
	@RequestMapping(value = "/list/{container}/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listConfigurations(
			@PathVariable("container") String container,
			@PathVariable("name") String name) {
		EntityManager entityManager = new EntityManager();
		List<String> schemas = entityManager
				.listConfigurations(container, name);
		return schemas;
	}

	/**
	 * 
	 * @return list of data paas frequencies which will be displayed as drop
	 *         down in UI
	 */
	@RequestMapping(value = "/listFrequency", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listFrequency() {
		List<String> frequencies = CommonResourceLoader.frequencyLoader();
		return frequencies;
	}

	/**
	 * 
	 * @return list of schema types which will be displayed as drop down in UI
	 */
	@RequestMapping(value = "/listSchemaType", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> listSchemaType() {
		List<String> schemaTypes = CommonResourceLoader.schemaTypeLoader();
		return schemaTypes;
	}

	/**
	 * 
	 * Data ingestion details this method list out log details for any data
	 * ingestion
	 * 
	 * @param entityId
	 * @return List<DataIngestionLog>
	 */

	@RequestMapping(value = "/listIngestionDetails/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody
	IngestionLogDetails listIngestionDetails(
			@PathVariable("entityId") Integer entityId) {

		IngestionLogDAO logDAO = new IngestionLogDAO();
		return logDAO.getLogObject(entityId);
	}

	

	@RequestMapping(value = "/getSchemaAuto", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	List<List<String>> getSchemaAuto(@RequestBody ExtendedDetails sObject,
			HttpServletResponse response) {

		String fName = sObject.getFileName();
		if (fName == null || fName.isEmpty()) {
			fName = FileReaderConstant.RDBMS_TYPE;
		}
/*		ExtendedDetails dbDetails = new ExtendedDetails();
		dbDetails.setDbType(sObject.getDbType());
		dbDetails.setDbName(sObject.getDbName());
		dbDetails.setTableName(sObject.getTableName());
		dbDetails.setHostName(sObject.getHostName());
		dbDetails.setPort(sObject.getPort());
		dbDetails.setUserName(sObject.getUserName());
		dbDetails.setPassword(sObject.getPassword());
		dbDetails.setColDeli(sObject.getColDeli());*/
		SampleDataView dataView = new SampleDataView(fName, sObject);
		List<List<String>> sampleData = new ArrayList<>();
		try {
			System.out
					.println("#############################\n##########################");
			sampleData = dataView.getSampleData();
			return sampleData;
		} catch (ZeasException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			System.out.println("***********************************");
			System.out
					.println("erorr**************************" + e.toString());
			response.setStatus(e.getErrorCode());
			try {
				response.getWriter().print(e.toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
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
	 * @author 19217
	 */
	@RequestMapping(value = "/PipelineJobInfo/{jobId}", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	PipelineJob getPipelineJobInfo(@PathVariable String pipelineJobId) {
		PipelineJob PipelineJob = null;
		PipelineJob = PipelineJobInfoController
				.getPipelieJobInfo(pipelineJobId);
		return PipelineJob;
	}

	/**
	 * this method helps to validate the validator for corresponding column
	 * 
	 * @param String
	 *            type, and entityId
	 * @return List<String>
	 */
	@RequestMapping(value = "/validatorValidation", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	List<String> validateValidator(@RequestBody Entity entity) {

		ValidatorErrorInfo errorInfo = new ValidatorErrorInfo();
		return errorInfo.getErrors(entity.getJsonblob());

	}

	/**
	 * service responsible for providing Map Reduce test run functionality
	 * 
	 * @param mapRedJarFile
	 *            jar file containing Mapper and Reducer class
	 * @param mapperCName
	 *            fully qualified class name of Mapper
	 * @param reducerCName
	 *            fully qualified class name of Reducer
	 * @param ipDataSetName
	 *            input data set name
	 * @return Success for successful map reduce execution Or Fail if map reduce
	 *         fails.
	 */
	@RequestMapping(value = "/uploadAndTestRun", method = RequestMethod.POST)
	public @ResponseBody
	String mapRedTesRun(@RequestParam("file") MultipartFile mapRedJarFile,
			@RequestParam("mapper") String mapperCName,
			@RequestParam("reducer") String reducerCName,
			@RequestParam("dataSetName") String ipDataSetName,
			@RequestParam("stageName") String stageName) {
		logger.debug("A request came to service mapRedTesRun");
		logger.debug("mapper: " + mapperCName + " reducer: " + mapperCName
				+ " ipDataSet: " + ipDataSetName);

		MapRedParam mapRedParam = new MapRedParam();
		mapRedParam.setMapperCName(mapperCName);
		mapRedParam.setReducerCName(reducerCName);
		mapRedParam.setIpDataSetName(ipDataSetName);
		mapRedParam.setStageName(stageName);
		MRTestRunManager mrTestRunManager = new MRTestRunManager();
		String testResult = mrTestRunManager
				.testRun(mapRedJarFile, mapRedParam);
		return testResult;
	}

	/**
	 * This method is to check the entered source location is already exist for
	 * any other ingestion or not.
	 * 
	 * @param entity
	 * @return boolean value: true if source location already exist for any
	 *         other ingestion.
	 */
	@RequestMapping(value = "/sourceLocationCheck", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Boolean sourceLocationCheck(@RequestBody Entity entity) {

		String srcLocation = entity.getName();
		if (srcLocation.endsWith("/")) {
			srcLocation = srcLocation.substring(0, srcLocation.length() - 1);
		}
		boolean isExist = false;
		List<String> sourceLocations = EntityManager.getSourceLocations();
		if (sourceLocations.contains(srcLocation)) {
			isExist = true;
		}
		return isExist;
	}

	/**
	 * This method is to check the entered source location is already exist for
	 * any other ingestion or not.
	 * 
	 * @param entity
	 * @return boolean value: true if source location already exist for any
	 *         other ingestion.
	 */
	@RequestMapping(value = "/schemaNameCheck", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Boolean schemaNameCheck(@RequestBody Entity entity) {

		String schemaName = entity.getName();
		boolean isExist = false;
		isExist = EntityManager.getDataschemaName(schemaName.toUpperCase());
		return isExist;
	}

	/**
	 * Method gets the list of columns for given dataset.It first checks the
	 * dataschema this dataset(table) is attributed with and then retrieves
	 * columns from dataschema.
	 * 
	 * @param tableName
	 *            {@link String} Name of the table.
	 * @return {@link List} of columns for a schema/table.
	 */
	@RequestMapping(value = "/getColumns/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> getColumnsForTable(@PathVariable String tableName) {
		List<String> columns = new ArrayList<String>();
		EntityManager em = new EntityManager();
		columns = em.getColumns(em.getSchemaName(tableName));
		return columns;
	}

	/**
	 * Verifying user access and creating target directories if not exists
	 * 
	 * @param entity
	 * @return
	 */
	@RequestMapping(value = "/verifyTargetHDFSPathAcess", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	Boolean verifyTargetHDFSPathAccess(@RequestBody Entity entity,
			HttpServletResponse response) {
		boolean validpath = false;
		String destDir = entity.getLocation();
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS",
					ConfigurationReader.getProperty("NAMENODE_HOST"));
			FileSystem hdfs = FileSystem.get(conf);
			Path hdfsDirPath = new Path(destDir);
			if (!(hdfs.exists(hdfsDirPath))) {
				hdfs.mkdirs(hdfsDirPath);
				logger.info("creating Target HDFS directory since it not exists");
			}
			validpath = true;
			return validpath;
		} catch (Throwable e) {
			logger.error("Do not have accesss" + e.toString());
			response.setStatus(ZeasErrorCode.FILE_NOT_FOUND);
			try {
				response.getWriter().print("User do not have access for HDFS");
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (Exception exception) {
				exception.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * This method takes care of delete Schema functionality. Its handled like
	 * this - a)move HDFS dataset from Target HDFS path to pre-defined local
	 * archive dir b)move definitions from Entity table to other table
	 * schema_archive
	 * 
	 * @param entity
	 * @return
	 */
	@RequestMapping(value = "/moveToArchive", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	void moveToArchive(@RequestBody Entity entity, HttpServletResponse response) {
		String destDir = entity.getLocation();
		String userName = entity.getCreatedBy();
		String schemaName = entity.getName();
		String ids = entity.getJsonblob();
		String[] str = ids.split(",");
		String dataSetId = str[0];
		String dataSourceId = str[1];
		String dataSchemaId = str[2];
		String dataSchedularId = str[3];
		try {
			EntityManager entitymanager = new EntityManager();
			entitymanager.moveToArchive(userName, dataSetId, dataSourceId,
					dataSchemaId, dataSchedularId, schemaName, destDir);
		} catch (Exception e) {
			String errorMessage = "some error happened at server";
			if (((ZeasException) e).getErrorCode() == ZeasErrorCode.SQL_EXCEPTION) {
				errorMessage = "Error in performing mysql database operations";
			} else if (((ZeasException) e).getErrorCode() == ZeasErrorCode.ZEAS_EXCEPTION) {
				errorMessage = "Error while accessing to HDFS";
			}
			logger.error("user cannot able to perfom move to archive"
					+ e.toString());
			response.setStatus(ZeasErrorCode.FILE_NOT_FOUND);
			try {
				response.getWriter().print(errorMessage);

			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (Exception exception) {
				exception.printStackTrace();
			}

		}

	}

	/**
	 * This service will provide list of archive profiles
	 * 
	 * @return map of schema id and name
	 * @throws ZeasException 
	 */
	@RequestMapping(value = "/ListArchiveProfiles", method = RequestMethod.GET, headers = "Accept=application/json")
	public HashMap<String, String> getArchiveProfiles(HttpServletResponse response) throws ZeasException {
		
		HashMap<String, String> profileList = null;
		try {
			EntityManager entityManager = new EntityManager();
			profileList = entityManager.getArchiveProfiles();
			logger.debug("getArchiveProfiles: Archive Profile List: "+ profileList);
			
		} catch (ZeasSQLException e) {
			response.setStatus(e.getErrorCode());
			try {
				response.getWriter().print(e.toString());
			} catch (IOException e1) {
				e1.printStackTrace();
			}	
		}	
		return profileList;
	}

	/**
	 * This service will restore the archive data - data is moved from archived
	 * location to hdfs location It also takes care of inserting schema, source,
	 * dataset and scheduler json information to database.
	 * 
	 * @param schema
	 *            id - schema that should be restored
	 */
	@RequestMapping(value = "/restoreArchivedData/{schemaDataId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public void restoreArchivedData(@PathVariable String schemaDataId,
			HttpServletResponse response) {
		
		int schemaId = Integer.parseInt(schemaDataId);
		PrintWriter printObj = null;
		logger.debug("restoreArchivedData: schemaid: " + schemaId);

		try {
			printObj = response.getWriter();
			EntityManager entityManager = new EntityManager();
			String hdfsPath = entityManager.getHdfsPath(schemaId);

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS",ConfigurationReader.getProperty("NAMENODE_HOST"));
			FileSystem hdfs = FileSystem.get(conf);
			Path hdfsDirPath = new Path(hdfsPath);
			
			if (!(hdfs.exists(hdfsDirPath))) {
				entityManager.restoreArchivedData(schemaId);
			} else {
				logger.info("EntityController: restoreArchivedData: hdfs path is already in use: ");
				response.setStatus(ZeasErrorCode.ZEAS_EXCEPTION);
				printObj.print("-Path: " + "\"" + hdfsPath + "\"" + " is already in use, please clean up data to import schema");
			}

    	} catch (ZeasSQLException e) {
    		e.printStackTrace();
    		response.setStatus(e.getErrorCode());
    		printObj.print(e.toString());
			logger.info("EntityController.restoreArchivedData(): ZeasSQLException: " + e.getMessage());	
    	} catch (ZeasException e) {
    		e.printStackTrace();
    		response.setStatus(e.getErrorCode());
    		printObj.print(e.toString());
			logger.info("EntityController.restoreArchivedData(): Exception: " + e.getMessage());	
    	} catch (Throwable e){
			logger.info("EntityController.restoreArchivedData(): Throwable: " + e.getMessage());
			response.setStatus(ZeasErrorCode.ZEAS_EXCEPTION);
			printObj.print("Processing Request Failed. Please check hadoop Configuration. Refer LOGS");
    	}
	}
}
