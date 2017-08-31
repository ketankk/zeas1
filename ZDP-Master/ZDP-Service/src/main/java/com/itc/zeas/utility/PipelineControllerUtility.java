/**
 * 
 */
package com.itc.zeas.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.json.JSONObject;

import com.itc.zeas.ingestion.automatic.rdbms.SqoopImportDetails;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.itc.zeas.model.ModuleSchema;

/**
 * @author Shishir Sarkar
 *
 */
final public class PipelineControllerUtility {
	final public static Logger LOGGER = Logger.getLogger(PipelineControllerUtility.class);

	/**
	 * @param xmlDocName
	 * @param inputPath
	 * @param outputPath
	 */
	public static final String FILE_TYPE ="file";
	
	public static final String RDBMS_TYPE ="rdbms";
	
	final public void executeHDFSPUTCommand(final String xmlDocName, final String inputPath, final String projectName,
			final String dataSetName) {
		LOGGER.info("Start migarting the xml file in HDFS location....");
		Process process = null;
		String outputPath = ConfigurationReader.getProperty("HDFS_USER_PATH") + projectName + "/" + dataSetName;

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
				process = Runtime.getRuntime().exec("hadoop dfs -put -f " + inputPath + "/" + xmlDocName + " " + outputPath+"/"+xmlDocName);
				process.waitFor();
				
				LOGGER.info("Succesfully migarted the xml file in HDFS location....");
			}
		} catch (Exception exception) {
			LOGGER.info("HDFS command failed..." + exception.getMessage());
		} finally {
			process.destroy();
		}
	}
	
		/**
	 * @param propertiesLocationName
	 * @param projectName
	 * @param timezone
	 * @param fileName 
	 */
	final public void doCreatetheCoordinatorConfigPropertiesFIle(String startTime, String endTime, String frequency,
			String dataSet, final String projectName, final String timezone, String fileName) {


		String rootPath = System.getProperty("user.home") + "/zeas/" + projectName + "/" + dataSet;
		String outputPath = ConfigurationReader.getProperty("HDFS_USER_PATH") + projectName + "/" + dataSet;
		
		File shellScriptFileName = new File(rootPath);
		if (!shellScriptFileName.exists()) {
			shellScriptFileName.mkdirs();
		}
		rootPath += "/coordinator.properties";
		shellScriptFileName = new File(rootPath);
		// oozie job --oozie http://54.210.74.58:11000/oozie -config
		// coordinator.properties -run
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
			builder.append("${nameNode}"+ConfigurationReader.getProperty("HDFS_USER_PATH")
							+ projectName + "/" + dataSet);
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
			builder.append("scriptName=");
			builder.append(fileName); 
			builder.append("\n");
			builder.append("scriptPath=");
			builder.append("${oozie.coord.application.path}/"+fileName+"#"+fileName); 
			builder.append("\n");

			builder.append("timezone=");
			builder.append(timezone);
			builder.append("\n");
			builder.append("hive_site_xml=");
			builder.append("/user/zeas/pipeline/hive-site.xml"); 
			builder.append("\n");
			
			builder.append("hiveScript=");
			builder.append("hive.hql"); 
			builder.append("\n");
			
			builder.append("fileschedulerJar=");
			builder.append("${nameNode}/");
			builder.append(outputPath);
			builder.append("/fileschduler.jar#fileschduler.jar"); 
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

	/**
	 * 
	 * @param schema
	 * @param oozieHttpUrl
	 */
	final public String doCreateShellScriptFile(String dataSet, String oozieHttpUrl, final String projectName) {
		// need to add the code for filepath
		/// home/z
		String rootPath = System.getProperty("user.home") + "/zeas/" + projectName + "/" + dataSet;
		File shellScriptFileName = new File(rootPath);
		if (!shellScriptFileName.exists()) {
			shellScriptFileName.mkdirs();
		}
		String fileName = rootPath +"/oozie.sh";
		
		shellScriptFileName = new File(fileName);
		
		try {
			Runtime.getRuntime().exec("chmod 777 "+fileName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		shellScriptFileName.setReadable(true, false);
		shellScriptFileName.setExecutable(true, false);
		shellScriptFileName.setWritable(true, false);
		
		// oozie job --oozie http://54.210.74.58:11000/oozie -config
		// coordinator.properties -run
		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {
			builder.append("#!/bin/bash");
			builder.append("\n");			
			builder.append("oozie job --oozie ");

			builder.append(oozieHttpUrl);
			builder.append(" -config  ");
			// nned to change the filename
			builder.append(rootPath+"/coordinator.properties -run");

			fileWriter = new FileWriter(shellScriptFileName);
			fileWriter.write(builder.toString());

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		return fileName;
	}

	/**
	 * 
	 * @param schema
	 * @param oozieHttpUrl
	 */
	final public void doCreateShellFileForMapReduce(String projectName, String dataSet, final String fileName) {
		// need to add the code for filepath
		// hadoop jar /var/tmp/Run.jar com.itc.zeas.ingestion.mapr.Run
		// /user/zeas/mapr/testdata.csv /user/zeas/maproutput
		// hadoop jar /tmp/databridge.jar com.itc.zeas.ingestion.mapr.Run
		// /user/zeas/mapr/testdata.csv /user/zeas/csvoutput
		final String databridgePath = System.getProperty("user.home") + "/zeas/";
		
		String inputPath = System.getProperty("user.home") + "/zeas/" + "Config/";
		File shellScriptFileName = new File(inputPath);
		if (!shellScriptFileName.exists()) {
			shellScriptFileName.mkdirs();
		}
		final String filePath = inputPath + "/mapreduce.sh";
		shellScriptFileName = new File(filePath);
		// oozie job --oozie http://54.210.74.58:11000/oozie -config
		// coordinator.properties -run
		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {
			builder.append("hadoop jar ");

			builder.append(databridgePath + "fileScheduler.jar");

			builder.append("com.itc.zeas.ingestion.mapr.Run ");
			builder.append(inputPath + "/" + fileName);
			
			builder.append(" " + ConfigurationReader.getProperty("HDFS_USER_PATH")
			+ projectName + "/" + dataSet+"$(date +%Y_%m_%dT%H_%M_%S)");
			
			fileWriter = new FileWriter(shellScriptFileName);
			fileWriter.write(builder.toString());

		} catch (IOException e) {
			e.printStackTrace();
			//return null;
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		//return inputPath + shellScriptFileName;
	}

	/**
	 * 
	 * @param shellFileName
	 * @param shellFilePath
	 */
	final public void runShellFile(final String shellFileName, final String shellFilePath) {
		LOGGER.info("Start executing the shell file....");
		Process process = null;
		try {
			if (StringUtils.isNotEmpty(shellFileName) || StringUtils.isNotEmpty(shellFilePath)) {
				process = Runtime.getRuntime().exec("./" + shellFilePath + "/" + shellFileName);
				LOGGER.info("Shell file executed successfully....");
			}
		} catch (Exception exception) {
			LOGGER.info("Failed to run the shell file!!! " + exception.getMessage());
		} finally {
			process.destroy();
		}
	}

	/**
	 * overloaded method
	 * 
	 * @param shellFileName
	 * @param shellFilePath
	 */
	final public void runShellFile(final String completeShellFilePath) {
		LOGGER.info("Start executing the shell file....");
		Process process = null;
		try {
			if (StringUtils.isNotEmpty(completeShellFilePath)) {
				process = Runtime.getRuntime().exec(completeShellFilePath);
				
				InputStream is = process.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				
				BufferedReader buff = new BufferedReader (isr);

				String line;

                System.out.println("buff output =="+buff.toString() + "=="+buff.readLine());

				while((line = buff.readLine()) != null)
					System.out.print(line);
				
				LOGGER.info("Shell file executed successfully....");
				process.waitFor();
			}
		} catch (Exception exception) {
			LOGGER.info("Failed to run the shell file!!! " + exception.getMessage());
		} finally {
			process.destroy();
		}
	}

	public static String constructFrequency(String freq, String repeats) {
		String frequency = null;

		switch (repeats) {
		case "months":
			frequency = "${coord:months(" + freq + ")}";
			break;
		case "hours":
			frequency = "${coord:hours(" + freq + ")}";
			break;

		case "days":
			frequency = "${coord:days(" + freq + ")}";
			break;
		default:
			frequency = "${coord:minutes(" + freq + ")}";
			break;
		}
		return frequency;
	}

	public String killJobScriptFile(String dataSet, String oozieHttpUrl, String projectName) {

		String workflowName = projectName + "_" + dataSet;

		String rootPath = System.getProperty("user.home") + "/zeas/" + projectName + "/" + dataSet;
		File shellScriptFileName = new File(rootPath);
		if (!shellScriptFileName.exists()) {
			shellScriptFileName.mkdirs();
		}
		rootPath += "/killJob.sh";
		shellScriptFileName = new File(rootPath);
		
		shellScriptFileName.setReadable(true, false);
		shellScriptFileName.setExecutable(true, false);
		shellScriptFileName.setWritable(true, false);
		
		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {
			builder.append("runningJob = oozie job --oozie ");
			builder.append(oozieHttpUrl);
			builder.append(" -jobtype");
			builder.append(" coordinator | grep -i " + workflowName);
			builder.append(" grep -i RUNNING ");
			builder.append(projectName + "_" + dataSet);
			builder.append(" | awk '{print$1}' ");
			builder.append("\n");
			builder.append("oozie job -oozie");
			builder.append(oozieHttpUrl);
			builder.append(" -kill $runningJob");

			fileWriter = new FileWriter(shellScriptFileName);
			
			fileWriter.write(builder.toString());

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
				}
			}
		}
		return rootPath + shellScriptFileName;
	}

	/**
	 * 
	 * @param doc
	 * @param outputPath
	 */
	private void saveXMLDoc(final Document doc, final String outputPath) {
		try {
			XMLOutputter xmlOutput = new XMLOutputter();

			xmlOutput.setFormat(Format.getPrettyFormat());
			xmlOutput.output(doc, new FileWriter(outputPath));

			LOGGER.info("XML File Saved!" + doc.getBaseURI());
		} catch (IOException io) {
			LOGGER.error("Exception occured in Pipeline controller to saving the xml file." + io.getMessage());
		}
	}

	public static String getDefaultTimeZone() {
		TimeZone timeZone = TimeZone.getDefault();
		return timeZone.getDisplayName();
	}
	public String getJobStatus(String jobName,String oozieURL,String fileName){
		
		String[] args = new String[2];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = fileName;
		args[2] = jobName;
		args[3] = oozieURL;
		
		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);
		
		ProcessBuilder  pb = new ProcessBuilder(args);
		   // Redirect the errorstream
		      pb.redirectErrorStream(true);
		      pb.redirectErrorStream(true);
		      Process p;
		    try {
		        p = pb.start();
		   
		      BufferedReader br = new BufferedReader(new InputStreamReader(
		              p.getInputStream()));
		      p.waitFor();
		      System.out.println("br output =="+br.toString() + "=="+br.readLine());
		      while (br.ready()) {
		          String str=br.readLine().trim();
		          System.out.println("str=="+str);
		      } 
		      //status=0;
		    } catch (IOException | InterruptedException e) {
		        e.printStackTrace();
		    } 
		    
		return null;
	}

	public String getFormatDate(String currentdate, String currentFormat, String targetFormat) {

		String dateString = currentdate;// "2016-10-28T11:55Z";
		String targetDate = "";
		Date date;
		try {
			date = new SimpleDateFormat(currentFormat).parse(dateString);
			targetDate = new SimpleDateFormat(targetFormat).format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return targetDate;
	}
	
	public static void main(String args[]){
		PipelineControllerUtility p= new PipelineControllerUtility();
		String schema = "(id int, design String,version int,created String,createdBy String,workspace_name String,projectType String)";
		String tableName = "sql_project";
		String location = "'/user/zeas/ccil_dev/sqldataset05/'";
		//p.hiveShellScript(schema,tableName,location,"sql_project","sqldataset05");
		
	}


	public String hiveShellScript(String schema, String tableName,String projectName,String dataset) {
		
		SqoopImportDetails details = new SqoopImportDetails();
		
		JSONObject jsonObj = details.getJsonObjectByName(dataset+"_DataSet");
		
		String location = jsonObj.getString("location") ;
		
		String rootPath = System.getProperty("user.home") + "/zeas/" + projectName + "/" + dataset;
		File hiveFile = new File(rootPath);
		if (!hiveFile.exists()) {
			hiveFile.mkdirs();
		}
		String fileName = rootPath +"/hive.hql";
		
		hiveFile = new File(fileName);
		
		hiveFile.setReadable(true, false);
		hiveFile.setExecutable(true, false);
		hiveFile.setWritable(true, false);
		
		FileWriter fileWriter = null;
		StringBuilder builder = new StringBuilder();
		try {

			builder.append("create external table if not exists zeas.");
			builder.append(tableName);
			builder.append("(");
			builder.append(schema);
			builder.append(")");
			builder.append("row format delimited");
			builder.append("\n");
			builder.append("fields terminated by','");
			builder.append("\n");
			builder.append("lines terminated by'\\n'");
			builder.append("\n");
			builder.append("location ");
			builder.append(" '");
			builder.append(location);
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
					details =null;
					jsonObj =null;
				} catch (IOException e) {
				}
			}
		}
		return rootPath;

		
	}

	public String schemaToString(List<ModuleSchema> schema) {
		StringBuilder builder = new StringBuilder();
		
		for(ModuleSchema m:schema){
			builder.append(m.getName());
			builder.append(" ");
			switch (m.getDataType()) {			
			case "long":
				builder.append("BIGINT");
				break;
			default:
				builder.append(m.getDataType());
			}
			
			builder.append(",");
		}
		return builder.substring(0,builder.lastIndexOf(","));
	}
	
}
