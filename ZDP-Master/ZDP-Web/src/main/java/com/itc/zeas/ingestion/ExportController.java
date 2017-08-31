package com.itc.zeas.ingestion;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.itc.zeas.utility.ExportControllerUtility;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.profile.daoimpl.HiveJdbcClient;
import com.itc.zeas.profile.model.Query;
import com.itc.zeas.profile.model.Entity;

@RestController
@RequestMapping("/rest/service")
public class ExportController {
	private static final Logger LOGGER = Logger.getLogger(ExportController.class);

	/**
	 * Below rest end point used to get the hive record
	 * 
	 * @param query
	 * @return list of hive record
	 * @throws ZeasSQLException
	 */
	@RequestMapping(value = "/getHiveResult", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody List<Map<String, String>> getResultByExecuteHiveQuery(@RequestBody Query query,
			HttpServletResponse response) {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		ExportControllerUtility exportControllerUtility = new ExportControllerUtility();
		String tableName = null;
		String hiveQuery;
		try {
			try {
				tableName = exportControllerUtility.getTableName(query.getEntityId());
			} catch (ZeasException e) {
				try {
					response.getWriter().print("Table doesn't exist in hive");
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			/* Building hive query */

			hiveQuery = exportControllerUtility.buildHiveQuery(tableName, query.getQuery());
			/* Calling hive client to fetch the record and return in list */
			list = new HiveJdbcClient().getResultByExecuteHiveQuery(hiveQuery);
		} catch (ZeasSQLException e) {
			response.setStatus(e.getErrorCode());
			LOGGER.error("Error in Query " + e.getMessage());

			try {
				response.getWriter().print(e.toString());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			try {
				response.getWriter().print("Internal Server Error");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * Below rest endpoint used to get the hive record
	 * 
	 * @param query
	 * @return list of hive record
	 *//*
		 * @RequestMapping(value = "/getDefaultHiveResult", method =
		 * RequestMethod.GET) public @ResponseBody List<Map<String, String>>
		 * getDefaultResultByExecuteHiveQuery() { List<Map<String, String>> list
		 * = new ArrayList<Map<String, String>>(); Calling hive client to fetch
		 * the record and return in list format list = new
		 * HiveJdbcClient().getResultByExecuteHiveQuery(
		 * "select * from demo_data_airline_dataset"); return list; }
		 * 
		 */
	/**
	 * rest service which provides file name and its path maping
	 * 
	 * @param datasetSchemaName
	 * @param httpServletRequest
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "/export", method = RequestMethod.GET, headers = "Accept=application/json")
	public void getFileListToBeDownloaded(@RequestParam("datasetSchemaName") String datasetSchemaName,
			HttpServletResponse response, HttpServletRequest request, @RequestParam(defaultValue = "csv") String format)
					throws IOException {
		LOGGER.debug("inside export service");
		System.out.println("datasetPath: " + datasetSchemaName);

		File file = null;
		/* Constructing Entity instance with valid location and dataset name */
		Entity entity = new ExportControllerUtility().populateEntityLocationAndDatasetName(datasetSchemaName);
		/* Exporting hive record into file */
		boolean fileExported = new ExportControllerUtility().isFileExported(entity, format);

		if (fileExported) {
			/* Changes as per discussion that _dataset will be handle by UI */
			if (format.equalsIgnoreCase("csv"))
				file = new File(entity.getLocation() + "/" + entity.getName() + ".csv");
			else {
				/* below logic use to create the file for .json only */
				file = new File(entity.getLocation() + "/" + entity.getName() + ".json");
			}

			/* File download logic */
			ServletContext context = request.getSession().getServletContext();
			FileInputStream inputStream = new FileInputStream(file);
			if (format.equalsIgnoreCase("JSON")) {
				int i = 0;
				StringBuilder stringBuilder = new StringBuilder();
				while ((i = inputStream.read()) != -1) {
					if ('}' == (char) i) {
						stringBuilder.append((char) i);
						stringBuilder.append(",");
					} else {
						stringBuilder.append((char) i);
					}
				}
				String jsonRecord = stringBuilder.toString();
				jsonRecord = jsonRecord.substring(0, jsonRecord.lastIndexOf(','));
				jsonRecord = '[' + jsonRecord + ']';
				try {
					BufferedWriter out = new BufferedWriter(new FileWriter(file));
					out.write(jsonRecord);
					out.close();
				} catch (IOException ioException) {
					LOGGER.error("Exception caught in Export Controller: " + ioException.getLocalizedMessage());
				}
				jsonRecord = null;
				stringBuilder = null;
			}
			inputStream.close();
			FileInputStream inputStream1 = new FileInputStream(file);
			response.setContentType(context.getMimeType(file.getAbsolutePath()));
			response.setContentLength((int) file.length());
			String headerKey = "Content-Disposition";
			String headerValue = String.format("attachment; filename=\"%s\"", file.getName());
			response.setHeader(headerKey, headerValue);

			// get output stream of the response
			OutputStream outStream = response.getOutputStream();

			byte[] buffer = new byte[(int) file.length()];
			int bytesRead = -1;

			// write bytes read from the input stream into the output stream
			while ((bytesRead = inputStream1.read(buffer)) != -1) {
				outStream.write(buffer, 0, bytesRead);
			}

			inputStream1.close();
			outStream.close();
			file.delete();
		}
	}
}
