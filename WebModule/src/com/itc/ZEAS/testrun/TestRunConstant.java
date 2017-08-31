package com.itc.zeas.testrun;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.itc.taphius.utility.DBUtility;

/**
 * contains constant getting used in Map Reduce test run
 * 
 * @author 19217
 * 
 */
public class TestRunConstant {
	private static Logger logger = Logger.getLogger(TestRunConstant.class);

	public static final String MAP_RED_TESTRUN_HOME;
	public static final String SHELL_SCRIPT_PATH;
	public static final String DRIVER_JAR_DIRECTORY;
	public static final String MAP_RED_JAR_DIR;
	public static final String LOCAL_INPUT_FILE_PATH;
	public static final String SHELL_SCRIPT_TYPE = "/bin/bash";
	public static final String DRIVER_JAR_NAME = "driver.jar";
	public static final String DRIVER_CLASS_NAME = "com.itc.zeas.mapred.driver.MapRedDriver";
	public static final String HDFS_URL;
	public static final String RESOURCE_MANAGER_URL;
	public static final String HDFS_IP_PATH = "/usr/local/hadoop/input/wordcount";
	public static final String HDFS_OP_PATH = "/usr/local/hadoop/output/wordcount";
	private static String RManagerUrl = "localhost:8032";
	private static String HdfsUrl = "hdfs://localhost:8020/";
	public static final String MAP_RED_JAR_PATH = "mapRedJarPath";
	static {
		initHdfsAndRManagerURL();
		MAP_RED_TESTRUN_HOME = System.getProperty("user.home")
				+ "/zeas/map_red_test_run/";
		SHELL_SCRIPT_PATH = MAP_RED_TESTRUN_HOME + "map_red_test_run.sh";
		DRIVER_JAR_DIRECTORY = MAP_RED_TESTRUN_HOME + "/Driver";
		MAP_RED_JAR_DIR = MAP_RED_TESTRUN_HOME + "MapRedJar/";
		LOCAL_INPUT_FILE_PATH = MAP_RED_TESTRUN_HOME + "SampleIpFile.txt";
		HDFS_URL = HdfsUrl;
		RESOURCE_MANAGER_URL = RManagerUrl;
	}

	// public static final String SHELL_SCRIPT_TYPE = "/bin/bash";
	// public static final String SHELL_SCRIPT_PATH =
	// "/home/hadoop/workplace/EclipseWorkspace/ToolMapRedRunner/src/Resource/Script/map_red_org.sh";
	// public static final String HDFS_URL = "hdfs://localhost:8020/";
	// public static final String RESOURCE_MANAGER_URL = "localhost:8032";
	// public static final String DRIVER_JAR_DIRECTORY =
	// "/home/hadoop/Deepak/03_06_15";
	// public static final String DRIVER_JAR_NAME = "driver.jar";
	// public static final String DRIVER_CLASS_NAME =
	// "com.itc.zeas.mapred.driver.MapRedDriver";
	// public static final String HDFS_IP_PATH =
	// "/usr/local/hadoop/input/wordcount";
	// public static final String HDFS_OP_PATH =
	// "/usr/local/hadoop/output/wordcount";
	// public static final String LOCAL_INPUT_FILE_PATH =
	// "/home/hadoop/workplace/EclipseWorkspace/MapReduceExecutor/src/Resource/SampleIpFile.txt";
	// public static final String MAP_RED_JAR_DIR =
	// "/home/hadoop/workplace/EclipseWorkspace/AutomatedRun/src/Resource/MapRedJar/";

	private static void initHdfsAndRManagerURL() {
		try {
		    InputStream inputStream = new FileInputStream(System.getProperty("user.home")+"/zeas/Config/config.properties");
            Properties prop = new Properties();
            prop.load(inputStream);
			RManagerUrl = prop.getProperty("JOB_TRACKER");
			HdfsUrl = prop.getProperty("HDFS_FQDN");
		} catch (IOException e) {
			logger.error("problem in reading content from config.properties file");
			e.printStackTrace();
		}
	}
}
