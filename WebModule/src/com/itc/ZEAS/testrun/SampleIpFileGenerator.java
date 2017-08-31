package com.itc.zeas.testrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.itc.taphius.utility.DBUtility;
import com.itc.zeas.filereader.DbReaderUtility;
import com.itc.zeas.filereader.JSONDataParser;

public class SampleIpFileGenerator {
	private Logger logger = Logger.getLogger(SampleIpFileGenerator.class);

	/**
	 * generate sample input file for given data set
	 * 
	 * @param dataSetName
	 *            name of input data set
	 * @return 0 represent success Or 1 represent failure
	 */
	public int generateSampleIpFile(String dataSetName) {
		logger.debug("inside function generateSampleIpFile");
		int ipFileGenResult = 0;
		try {
			Connection conn = DBUtility.getConnection();
			// TODO below line needs to be deleted
			// dataSetName = "emp_dataset";// date22
			Map<String, String> colNameAndValue = DbReaderUtility
					.getDBColumnNameAndValue(conn,
							"select json_data from entity where name='"
									+ dataSetName + "'");
			JSONObject jsonObject = new JSONObject(
					colNameAndValue.get("JSON_DATA"));
			String schemaName = jsonObject.getString("Schema");
			logger.debug("Schema for Dtataset " + dataSetName + " is:"
					+ schemaName);

			// TODO below line needs to be removed
			// schemaName = "emp";// date22
			//conn = DBUtility.getConnection();
			colNameAndValue = DbReaderUtility.getDBColumnNameAndValue(conn,
					"select json_data from entity where name='" + schemaName
							+ "'");
			logger.debug("::::::::::" + colNameAndValue);
			JSONDataParser dataParser = new JSONDataParser();
			Map<String, String> colValidatorMap = dataParser
					.JsonParser(colNameAndValue.get("JSON_DATA"));
			logger.debug("list of validtor");
			logger.debug("column count" + colValidatorMap.size());
			logger.debug("value:----" + colValidatorMap);
			logger.debug("results");
			String sampleFileContents = DbReaderUtility
					.getSampleContentForSchema(colValidatorMap);
			logger.debug(sampleFileContents);
			String localFleName = TestRunConstant.LOCAL_INPUT_FILE_PATH;
			// TODO below lines needs to be deleted
			// localFleName = "D:\\Software\\testrundel\\dest" + "sam.txt";
			File sampleIpFile = new File(localFleName);
			// delete the old sample input file if any
			if (sampleIpFile.exists()) {
				sampleIpFile.delete();
			}
			try (PrintWriter printWriter = new PrintWriter(sampleIpFile)) {
				printWriter.println(sampleFileContents);
				logger.debug("sample File generation is successful");
				ipFileGenResult = 1;
			} catch (FileNotFoundException e) {
				logger.error("file: " + sampleIpFile + " has not fond");
				e.printStackTrace();
			}
		} catch (Exception exception) {
			logger.error("problem while sample File generation");
		}
		return ipFileGenResult;
	}

	// public static void main(String[] args) {
	//
	// SampleIpFileGenerator fileGenerator = new SampleIpFileGenerator();
	// // fileGenerator.generateSampleIpFile();
	// }
}
