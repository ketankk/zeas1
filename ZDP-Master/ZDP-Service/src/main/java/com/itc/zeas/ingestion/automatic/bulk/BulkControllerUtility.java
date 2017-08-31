package com.itc.zeas.ingestion.automatic.bulk;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.itc.zeas.ingestion.automatic.rdbms.SqoopImportDetails;
import com.itc.zeas.utility.utility.ConfigurationReader;



public class BulkControllerUtility {
	private static final Logger LOGGER = Logger.getLogger(BulkControllerUtility.class);
	
	
	public static void main(String args[]){
		String outputPath ="/user/zeas/chaitra/Profiles/"
				+ "projectName" + "/" + "dataSetName";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", ConfigurationReader.getProperty("NAMENODE_HOST"));
	 
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path path = new Path(outputPath);

			if (!fileSystem.exists(path)) {
				fileSystem.mkdirs(path);
			}
		} catch (IOException e) {
			LOGGER.error("Exception to create the output directory in hdfs location!!! " + e.getMessage());
		}
	}
	
		public void executeHDFSPUTCommand(final String xmlDocName, final String inputPath, final String projectName,
			final String dataSetName, String userName) {
		LOGGER.info("Start migarting the xml file in HDFS location....");
		Process process = null;
		String outputPath = ConfigurationReader.getProperty("HDFS_USER_PATH") + "/" + userName + "/Profiles/"
				+ projectName + "/" + dataSetName;
	
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", ConfigurationReader.getProperty("NAMENODE_HOST"));
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			Path path = new Path(outputPath);

			if (!fileSystem.exists(path)) {
				fileSystem.mkdirs(path);
			}
		} catch (IOException e) {
			LOGGER.error("Exception to create the output directory in hdfs location!!! " + e.getMessage());
		}

		/// user/zeas/<project_name>/<dataset_name>/
		// dir will not exist need to check isExist if not create dir in hdfs

		try {
			if (StringUtils.isNotEmpty(xmlDocName) || StringUtils.isNotEmpty(inputPath)
					|| StringUtils.isNotEmpty(outputPath)) {
				process = Runtime.getRuntime().exec(
						"hadoop dfs -put -f " + inputPath + "/" + xmlDocName + " " + outputPath + "/" + xmlDocName);
				
				process.waitFor();

				LOGGER.info("Succesfully migarted the xml file in HDFS location....");
			}
		} catch (Exception exception) {
			LOGGER.info("HDFS command failed..." + exception.getMessage());
		} finally {
			process.destroy();
		}
	}

	public void coordinatorConfigProperties(String startTime, String endTime, String frequency, String dataSet,
			final String projectName, final String timezone, String fileName, String userName, String sourceType,
			String schema, String header) {
		String rootPath = System.getProperty("user.home") + "/zeas/" + userName + "/Profiles/" + projectName + "/"
				+ dataSet;
		File shellScriptFileName = new File(rootPath);
		if (!shellScriptFileName.exists()) {
			shellScriptFileName.mkdirs();
		}
		rootPath += "/coordinator.properties";
		shellScriptFileName = new File(rootPath);

		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {
			builder.append("nameNode=");
			builder.append(ConfigurationReader.getProperty("NAMENODE_HOST"));
			builder.append("\n");
			builder.append("jobTracker=");
			builder.append(ConfigurationReader.getProperty("JOB_TRACKER"));
			builder.append("\n");
			builder.append("queueName=default");
			builder.append("\n");
			builder.append("oozie.coord.application.path=");
			builder.append("${nameNode}" + ConfigurationReader.getProperty("HDFS_USER_PATH") + userName
					+ "/Profiles/" + projectName + "/" + dataSet);
			builder.append("\n");
			builder.append("oozie.libpath=");
			builder.append("${nameNode}" + ConfigurationReader.getProperty("HDFS_USER_PATH") + userName
					+ "/Profiles/" + projectName + "/" + dataSet + "/" + "lib");
			builder.append("\n");
			builder.append("oozie.use.system.libpath=true");
			builder.append("\n");
			builder.append("startTime=");
			builder.append(startTime);
			builder.append("\n");
			builder.append("endTime=");
			builder.append(endTime);
			builder.append("\n");
			builder.append("frequency=");
			builder.append(frequency);
			builder.append("\n");
			builder.append("timezone=");
			builder.append(timezone);
			builder.append("\n");
			builder.append("hive_site_xml="+ ConfigurationReader.getProperty("HDFS_USER_PATH") + userName
					+ "/Profiles/" + projectName + "/" + dataSet+"/hive-site.xml");
			builder.append("\n");
			builder.append("hiveScript=hive.hql");
			builder.append("\n");

			builder.append("scriptName=SchedulerSqoopImport.sh");
			builder.append("\n");
			builder.append("scriptPath=${nameNode}" + ConfigurationReader.getProperty("HDFS_USER_PATH") + userName
					+ "/Profiles/" + projectName + "/" + dataSet
					+ "/SchedulerSqoopImport.sh#SchedulerSqoopImport.sh");
			builder.append("\n");

			builder.append("arg0=");
			builder.append(ConfigurationReader.getProperty("HDFS_USER_PATH") + userName + "/Profiles/" + projectName
					+ "/" + dataSet + "/");
			builder.append("\n");
			builder.append("arg1=");
			builder.append(dataSet);
			builder.append("\n");
			builder.append("arg2=");
			builder.append("output/");
			builder.append("\n");
			builder.append("arg3=");
			builder.append(sourceType);
			builder.append("\n");
			builder.append("arg4=");
			builder.append(frequency);
			builder.append("\n");
			builder.append("arg5=0316");

			builder.append("\n");
			builder.append("arg6=1603");

			builder.append("\n");
			builder.append("arg7=input");

			builder.append("\n");
			builder.append("arg8=");
			builder.append(header);
			builder.append("\n");
			builder.append("arg9=");
			builder.append(schema);
			builder.append("\n");
			builder.append("arg10=");
			builder.append(userName);
			builder.append("\n");

			fileWriter = new FileWriter(shellScriptFileName);
			fileWriter.write(builder.toString());

			LOGGER.info("Properties file has been created in linux location....");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}

	}

	public String hiveShellScript(String schema, String tableName, String profileName, String jobName, String userName,
			String SourceType) {

		SqoopImportDetails details = new SqoopImportDetails();

		JSONObject jsonObjSource = details.getJsonBulkObjectByName(jobName);

		String location = jsonObjSource.getString("location");

		String rootPath = System.getProperty("user.home") + "/zeas/" + userName + "/Profiles/" + profileName + "/"
				+ jobName;
		File hiveFile = new File(rootPath);
		if (!hiveFile.exists()) {
			hiveFile.mkdirs();
		}
		String fileName = rootPath + "/hive.hql";

		hiveFile = new File(fileName);

		hiveFile.setReadable(true, false);
		hiveFile.setExecutable(true, false);
		hiveFile.setWritable(true, false);

		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {

			builder.append("set hive.mapred.supports.subdirectories=true;");
			builder.append("\n");
			builder.append("set mapred.input.dir.recursive=true;");
			builder.append("\n");
			builder.append("create external table if not exists zeas.");
			builder.append(tableName);
			builder.append("(");
			builder.append(schema);
			builder.append(")");
			builder.append("\n");
			builder.append("row format delimited");
			builder.append("\n");
			if (SourceType.equalsIgnoreCase("xlsx") || SourceType.equalsIgnoreCase("xls")) {
				builder.append("fields terminated by '\\t'");
			} else {
				builder.append("fields terminated by ','");
			}
			builder.append("\n");
			builder.append("lines terminated by'\\n'");
			builder.append("\n");
			builder.append("location ");
			builder.append(" '");
			if (SourceType.equalsIgnoreCase("csv")) {
				builder.append(location + "/output/cleansed/");
			}else{
			builder.append(location + "/output/");
			}
			builder.append("';");

			fileWriter = new FileWriter(hiveFile);
			fileWriter.write(builder.toString());

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
					details = null;
					jsonObjSource = null;
				} catch (IOException e) {
				}
			}
		}
		return rootPath;

	}

}

