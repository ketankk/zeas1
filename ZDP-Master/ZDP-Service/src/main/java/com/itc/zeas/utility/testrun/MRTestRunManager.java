package com.itc.zeas.utility.testrun;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.web.multipart.MultipartFile;

public class MRTestRunManager {
	private Logger logger = Logger.getLogger(MRTestRunManager.class);
	private MapRedParam mapRedParam;



	/**
	 * 
	 * @param mapRedJarFile
	 *            uploaded Jar file containing Mapper and Reducer class
	 * @param mapRedParam
	 *            instance of MapRedParam
	 * @return 'Success' represent success Or 'Fail' represent failure
	 */
	public String uploadJar(MultipartFile mapRedJarFile, MapRedParam mapRedParam) {
		logger.debug("inside function testrun");

		this.mapRedParam = mapRedParam;

		return copyJarToLocalMachine(mapRedJarFile);
	}
	/**
	 * Start Map Reduce test run by executing run script
	 * 
	 * @return 'Success' represent success Or 'Fail' represent failure
	 */
	private String startTestRun() {
		logger.debug("inside function startTestRun");
		String testRunResult = "fail";
		String[] commands = { TestRunConstant.SHELL_SCRIPT_TYPE,
				TestRunConstant.SHELL_SCRIPT_PATH,
				TestRunConstant.DRIVER_JAR_DIRECTORY,
				TestRunConstant.DRIVER_JAR_NAME,
				TestRunConstant.DRIVER_CLASS_NAME,
				TestRunConstant.LOCAL_INPUT_FILE_PATH,
				TestRunConstant.HDFS_IP_PATH, TestRunConstant.HDFS_OP_PATH,
				mapRedParam.getMapperCName(), mapRedParam.getReducerCName(),
				TestRunConstant.HDFS_URL, TestRunConstant.RESOURCE_MANAGER_URL,
				mapRedParam.getMapRedJarPath() };// com.itc.zeas.mapred.driver.MapRedDriver
		for (String command : commands) {
			logger.debug("param passed to js" + command);
		}

		ProcessBuilder processBuilder = new ProcessBuilder(commands);
		Process mapRedProcess = null;
		try {
			mapRedProcess = processBuilder.start();
			logger.info("process to run map reduce shell script has been started");
		} catch (IOException e) {
			logger.error("problem while creating instance of class java.lang.Process to run map reduce shell script");
			e.printStackTrace();
		}

		// logs the shell output log
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(mapRedProcess.getInputStream()));
		String line = "";
		try {
			while ((line = bufferedReader.readLine()) != null) {
				System.out.println(line);
				logger.info(line);
				if (line.contains("result of Map Reduce test run: ")) {
					testRunResult = line.replace(
							"result of Map Reduce test run: ", "");
					logger.info("test run result: " + testRunResult);
				}
				 //bufferedReader.close();
			}
		} catch (IOException e) {
			logger.error("IOException while reading log stream for process which is running process script");
			e.printStackTrace();
		}
		//closing bufferedReader
        finally{
        	if (bufferedReader != null)
        	{
        		try {
					bufferedReader.close();
				} catch (IOException e) {	
					e.printStackTrace();
				}
        	}
        }
		// logs the error
		bufferedReader = new BufferedReader(new InputStreamReader(
				mapRedProcess.getErrorStream()));
		try {
			while ((line = bufferedReader.readLine()) != null) {
				System.out.println("SCRIPT_ERROR:" + line);
				logger.info(line);
			}
		} catch (IOException e) {
			logger.error("IOException while reading eror log stream for process which is running process script");
			e.printStackTrace();
		}
		//closing bufferedReader
		finally{
        	if (bufferedReader != null)
        	{
        		try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        }
		return testRunResult;
	}

	/**
	 * Copy uploaded Jar file to local machine
	 * 
	 * @param mapRedJarFile
	 *            Jar file containing Mapper and Reducer class
	 * @return 0 represent success Or 1 represent failure
	 */
	private String copyJarToLocalMachine(MultipartFile mapRedJarFile) {
		logger.debug("inside function copyJarToLocalMachine");		
		String jarFilePath = "";
		if (!mapRedJarFile.isEmpty()) {
			try {
				Date date = new Date();
				long executionTime = date.getTime();
				
				jarFilePath = TestRunConstant.MAP_RED_JAR_DIR
						+ mapRedJarFile.getName()+executionTime + ".jar";
				logger.debug("jarFilePath: " + jarFilePath);
				byte[] bytes = mapRedJarFile.getBytes();
				BufferedOutputStream stream = new BufferedOutputStream(
						new FileOutputStream(new File(jarFilePath)));
				stream.write(bytes);
				stream.close();
				logger.debug("jar file copied successfully");
				// Updating entity table with jar path for stage details
				Thread.sleep(3000);

			} catch (Exception e) {
				logger.error("problem while coping uploaded map reduce jar file to local machine");
				e.printStackTrace();
			}
		} else {
			logger.error("map reduce jar file is empty");
		}
		return jarFilePath;
	}
}
