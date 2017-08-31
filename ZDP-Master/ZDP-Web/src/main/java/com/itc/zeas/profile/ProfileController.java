package com.itc.zeas.profile;

import com.itc.zeas.ingestion.automatic.bulk.BulkEntityManager;
import com.itc.zeas.ingestion.automatic.bulk.EntityManagerController;
import com.itc.zeas.ingestion.automatic.bulk.EntityManagerInterface;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.ingestion.automatic.file.parsers.ExcelFileParser;
import com.itc.zeas.profile.model.BulkEntity;
import com.itc.zeas.profile.model.BulkProfile;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.profile.model.Profile;
import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.model.ModuleSchema;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.model.DatasetPathDetails;
import com.itc.zeas.utility.utility.UserProfileStatusCache;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/service")
public class ProfileController {

	private static final Logger LOG = Logger.getLogger(ProfileController.class);

	@Resource(name = "entityManager")
	EntityManager entityManager;

	/**
	 * This service will provide list of Profile , every Profile is
	 * representation of consolidated information about data source, data
	 * set,schema and scheduler
	 *
	 * @return list of Profile
	 */

	@RequestMapping(value = "/profiles", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getProfile(HttpServletRequest httpServletRequest, HttpServletResponse response)
			throws IOException {
		// EntityManager entityManager = new EntityManager();
		List<Profile> profileList = null;
		try {
			profileList = entityManager.getProfiles(httpServletRequest);
			LOG.info("Profile List is " + profileList);
			return ResponseEntity.ok(profileList);
		} catch (NullPointerException e) {
			response.getWriter().print("NullPointerException occured while getting profiles ");
			response.setStatus(ZeasErrorCode.NULL_POINTER_EXCEPTION);
			LOG.error("Exception while accessing /profiles api" + e.getMessage());
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(e.getMessage());

		} catch (Exception e) {
			LOG.error("Exception while accessing /profiles api" + e.getMessage());
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	@RequestMapping(value = "/bulkProfiles/{profileName}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getBulkProfiles(@PathVariable("profileName") String profileName,
			HttpServletRequest httpServletRequest) {
		BulkEntityManager bulkEntityManager = new BulkEntityManager();
		List<BulkProfile> bulkProfileList = null;
		try {
			bulkProfileList = bulkEntityManager.getBulkProfiles(httpServletRequest, profileName);
		} catch (Exception e) {
			LOG.info(e.getMessage());
			e.printStackTrace();
		}
		return ResponseEntity.ok(bulkProfileList);
	}

	@RequestMapping(value = "/getprofileRunStatus", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getprofileRunStatus(HttpServletRequest httpServletRequest) {
		EntityManager entityManager = new EntityManager();
		CommonUtils commonUtils = new CommonUtils();
		String accessToken = commonUtils.extractAuthTokenFromRequest(httpServletRequest);
		String userId = commonUtils.getUserNameFromToken(accessToken);
		Map<Long, String> profileList = null;
		try {
			profileList = entityManager.getProfileRunStatus(userId);
		} catch (Exception e) {
			LOG.error("Exception occurred while getting profile run status " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(profileList);
	}

	/**
	 * This api is called at the time of profile/(dataset) creation
	 *
	 * @return Entity
	 * @throws IOException
	 * @throws ZeasException
	 * @throws ParseException
	 * @throws JSONException
	 */
	@RequestMapping(value = "/addEntity", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Entity addEntity(@RequestBody Entity entity, HttpServletRequest request,
			HttpServletResponse response) throws ZeasSQLException, IOException {
		try {
			EntityManagerController entityManagerCotroller = new EntityManagerController();
			EntityManagerInterface entityManagerInterface = entityManagerCotroller
					.getEntityManagerInterfaceInstance(entity.getType());
			entityManagerInterface.addEntity(entity, request);
		} catch (Exception e) {
			LOG.info("Exception happened while adding entity " + entity);
			LOG.error("Exception occurred is " + e.getMessage());

		}

		return entity;

	}

	/**
	 * This method is to check the entered source location is already exist for
	 * any other ingestion or not.
	 *
	 * @param entity
	 * @return boolean value: true if source location already exist for any
	 *         other ingestion.
	 * @throws IOException
	 */
	@RequestMapping(value = "/sourceLocationCheck", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Boolean sourceLocationCheck(@RequestBody Entity entity) throws IOException {

		String srcLocation = entity.getName();
		if (srcLocation.endsWith("/")) {
			srcLocation = srcLocation.substring(0, srcLocation.length() - 1);
		}
		boolean isExist = false;
		List<String> sourceLocations;
		try {
			sourceLocations = EntityManager.getSourceLocations();
			if (sourceLocations.contains(srcLocation)) {
				isExist = true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return isExist;
	}

	/**
	 * @param entity
	 * @return boolean value: true if schema name already exist for any other
	 *         ingestion.
	 * @throws IOException
	 */
	@RequestMapping(value = "/schemaNameCheck", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Boolean schemaNameCheck(@RequestBody Entity entity) throws IOException {

		String schemaName = entity.getName();
		boolean isExist = false;
		try {
			isExist = EntityManager.getDataschemaName(schemaName.toUpperCase());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isExist;
	}

	@RequestMapping(value = "/bulkNameCheck", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody Entity bulkNameCheck(@RequestBody Entity entity) throws IOException {

		BulkEntityManager bem = new BulkEntityManager();
		entity = bem.validation(entity);
		return entity;
	}

	/**
	 * Method gets the list of columns for given dataset.It first checks the
	 * dataschema this dataset(table) is attributed with and then retrieves
	 * columns from dataschema.
	 *
	 * @param tableName
	 *            {@link String} Name of the table.
	 * @return {@link List} of columns for a schema/table.
	 * @throws SQLException
	 * @throws IOException
	 */
	@RequestMapping(value = "/getColumns/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
	public List<String> getColumnsForTable(@PathVariable String tableName) throws IOException, SQLException {
		List<String> columns = new ArrayList<String>();
		EntityManager em = new EntityManager();
		try {
			columns = em.getColumns(em.getSchemaName(tableName));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return columns;
	}

	/**
	 * @return getTablesList
	 */
	@RequestMapping(value = "/getTablesList", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody List<String> getTablesList(@RequestBody ExtendedDetails sObject,
			HttpServletResponse response) {
		RdmsMultipleTablesReader rmtr = new RdmsMultipleTablesReader();
		List<String> tableList = null;
		try {
			System.out.println("Getting table list for given info.");
			tableList = rmtr.getTablesList(sObject);
			return tableList;
		} catch (ZeasException e) {
			System.out.println("***********************************");
			System.out.println("erorr**************************" + e.toString());
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
	 * @return {@link List} of columns for a schema/table.
	 */
	@RequestMapping(value = "/getColumnsById/{datasetId}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getColumnsForDataset(@PathVariable String datasetId) {
		List<ModuleSchema> columns = new ArrayList<ModuleSchema>();
		// EntityManager em = new EntityManager();
		// columns =
		// em.getColumns(em.getSchemaName(Integer.parseInt(datasetId)));
		ZDPDataAccessObjectImpl dao = new ZDPDataAccessObjectImpl();
		try {
			columns = dao.getColumnAndDatatype(datasetId);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(columns);
	}

	/**
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	@RequestMapping(value = "/createEntities", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody void createProfileForTables(@RequestBody ExtendedDetails sObject, HttpServletResponse response)
			throws IOException, SQLException {
		RdmsMultipleTablesReader rmtr = new RdmsMultipleTablesReader();

		try {
			System.out.println("Inserting the Entities into DB for the tables");
			rmtr.CreateProfileForTheTables(sObject);
			try {
				response.getWriter().print("Profile creation successfully completed for "
						+ sObject.getSelectedTable().size() + " tables.");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (ZeasException e) {
			System.out.println("***********************************");
			System.out.println("erorr**************************" + e.toString());
			response.setStatus(e.getErrorCode());
			try {
				response.getWriter().print(e.toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * This service will provide sample data preview for local file upload take
	 * the multipart request and another parameters to parse the file show in UI
	 * here it is creating two threads, one for sample file creation for preview
	 * and another for complete file copy to zeas server.
	 *
	 * @param multipartRequest
	 * @return
	 * @throws ZeasException
	 */
	@RequestMapping(value = "/getSchemaForLocalFileUpload", method = RequestMethod.POST)
	public @ResponseBody ResponseEntity<?> getSchemaForUploadFile(MultipartHttpServletRequest multipartRequest) {
		MultipartFile multipartFile = multipartRequest.getFile("file");
		String authToken = multipartRequest.getHeader("X-Auth-Token");
		/* If token not found get it from request parameter */
		if (authToken == null) {
			authToken = multipartRequest.getParameter("token");
		}
		String userName = authToken.split(":")[0];
		String profileName = multipartRequest.getParameter("profileName");
		String fileName = multipartRequest.getParameter("fileName");
		String fileType = multipartRequest.getParameter("fileType");
		String format = multipartRequest.getParameter("format");
		String hFlag = multipartRequest.getParameter("hFlag");
		String noOfColumn = multipartRequest.getParameter("noOfColumn");
		String fixedValues = multipartRequest.getParameter("fixedValues");
		String colDeli = multipartRequest.getParameter("colDeli");
		String rowDeli = multipartRequest.getParameter("rowDeli");
		//
		ExtendedDetails sObject = new ExtendedDetails();
		sObject.setFileName(fileName);
		sObject.setFileType(fileType);
		sObject.setFileType(format);
		sObject.setmFlag("false");
		sObject.sethFlag(hFlag);
		if (!("".equalsIgnoreCase(noOfColumn) || noOfColumn == null)) {
			sObject.setNoOfColumn(Integer.parseInt(noOfColumn));
		} else {
			sObject.setNoOfColumn(0);
		}

		sObject.setFixedValues(fixedValues);
		colDeli = rowDeli.replaceAll("[^\\p{Alpha}]+", "");
		rowDeli = colDeli.replaceAll("[^\\p{Alpha}]+", "");
		sObject.setRowDeli(rowDeli);
		sObject.setColDeli(colDeli);
		String lPath = System.getProperty("user.home") + "/uploadData/" + profileName + "/";
		// String
		// sPath=System.getProperty("user.home")+"/uploadData/"+profileName+"/sample/";
		String lFilePath = lPath + multipartFile.getOriginalFilename();
		// String sFilePath=sPath+multipartFile.getOriginalFilename();
		File dir = new File(lPath);
		// File sdir=new File(sPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		/*
		 * if(!sdir.exists()){ sdir.mkdirs(); }
		 */
		// Add new ingestion profile to the cached map.
		UserProfileStatusCache.addKeyToMap(userName + "-" + profileName);

		// creating thread to start copy of complete file to server.
		UploadFile upload = new UploadFile(multipartFile, true, lFilePath, profileName, userName);

		// creating thread to copy sample portion of complete file to server.
		UploadFile ingestion = new UploadFile(multipartFile, false, lFilePath, profileName, userName);
		upload.start();
		ingestion.start();

		while (true) {
			if (!upload.isAlive()) {
				break;
			}
		}

		// Getting sample data from file to preview in UI.
		SampleDataView dataView = new SampleDataView(lFilePath, sObject, null, null);
		List<List<String>> sampleData = null ;
		try {

			sampleData = dataView.getSampleData();
		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());

		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(sampleData);
	}

	/**
	 * Method to fetch dataset path details info. It carries info like whether
	 * Transparent encryption is enabled or not, what is rootPath for dataset,
	 * and encryptionZone path
	 *
	 * @return path details in {@link DatasetPathDetails}
	 * @throws Exception
	 */
	// rest api call for bulk upload
	@RequestMapping(value = "/getSchemaForBulkUpload", method = RequestMethod.POST)
	public @ResponseBody Entity getSchemaForBulkUpload(MultipartHttpServletRequest multipartRequest) {
		MultipartFile multipartFile = multipartRequest.getFile("file");
		String authToken = multipartRequest.getHeader("X-Auth-Token");
		/* If token not found get it from request parameter */
		if (authToken == null) {
			authToken = multipartRequest.getParameter("token");
		}
		String userName = authToken.split(":")[0];
		String profileName = multipartRequest.getParameter("profileName");
		String type = multipartRequest.getParameter("profileType");
		String lPath = System.getProperty("user.home") + "/bulkMetaData/" + profileName + "/";

		String lFilePath = lPath + multipartFile.getOriginalFilename();
		File dir = new File(lPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		UserProfileStatusCache.addKeyToMap(userName + "-" + profileName);
		UploadFile upload = new UploadFile(multipartFile, true, lFilePath, profileName, userName);
		upload.start();
		ExcelFileParser parser = new ExcelFileParser();
		Entity entity = new Entity();
		List<List<Object>> fulldata;
		try {
			fulldata = parser.excelData(lFilePath);
			entity.setName(profileName);
			entity.setCreatedBy(userName);
			entity.setUpdatedBy(userName);
			entity.setJsonblobForBulk(fulldata);
			entity.setType(type);
		} catch (Exception e) {
			LOG.info("Exception" + e.getMessage());
			e.printStackTrace();
		}
		return entity;

	}

	// TODO
	// need to check this implementation
	@ExceptionHandler(value = Exception.class)
	@ResponseBody
	public Exception handleIOException(HttpServletResponse response, Exception ex) {
		LOG.error("Excetion occurred...." + ex.getMessage());
		if (ex instanceof Exception) {
			response.setStatus(ZeasErrorCode.DUPLICATE_ENTITY);
		}
		return null;

	}

}
