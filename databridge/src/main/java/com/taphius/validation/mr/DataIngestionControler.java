package com.taphius.validation.mr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.mortbay.log.Log;

import com.itc.zeas.custominputformat.CustomTextInputFormat;
import com.itc.zeas.custominputformat.DelimitedFileInputFormat;
import com.itc.zeas.custominputformat.ExcelInputFormat;
import com.itc.zeas.custominputformat.FixedInputFormat;
import com.itc.zeas.custominputformat.JsonInputFormat;
import com.itc.zeas.custominputformat.XmlInputFormat;
import com.itc.zeas.validation.rule.DataTypeCheckUtility;
import com.itc.zeas.validation.rule.DataValidationConstant;
import com.itc.zeas.validation.rule.JSONDataParser;
import com.itc.zeas.validation.rule.JsonColumnValidatorParser;
import com.itc.zeas.validation.rule.ValidationAttribute;
import com.taphius.databridge.utility.ZDPLog;
import com.zdp.dao.ZDPDataAccessObjectImpl;

public class DataIngestionControler extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(DataIngestionControler.class);

	private String records_processed = "";
	private String json = "";
	// private String json="";
	private IngestionLogDetails logDetails;
	private String schedularName = "";
	private StringBuilder logInfo = new StringBuilder();
	private String mapPercentage = "";
	public static ZDPLog zdpLog = ZDPLog.getZDPLog("DataIngestionControler");

	// end
	public DataIngestionControler(String scheduleName) {
		this.schedularName = scheduleName;
	}

	public static enum RECORDS_PROCESSED {
		TOTAL, CLEANSED, noOfRangeFails, noOfRegexFails, noOfFixedlLengthFails, noOfWhiteListFails, noOfBlackListFails, noOfMandatoryFails, noOfDataTypeMismatchFails, noOfColumnMismatchFails, noOfotherFails,

	};

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// logInfo.append("reaches to driver run......\n");
		logInfo.append(zdpLog.INFO("Validation started....for schedular:" + schedularName));
		// cathc error
		LoggedPrintStream lpsOut = LoggedPrintStream.create(System.out);
		LoggedPrintStream lpsErr = LoggedPrintStream.create(System.err);
		List<String> users = new ArrayList<String>();
		ZDPDataAccessObjectImpl accessObjectActivity = new ZDPDataAccessObjectImpl();

		// Set them to stdout / stderr
		System.setOut(lpsOut);
		System.setErr(lpsErr);
		// args = new String[9];
		/**
		 * args[0]= hdfs base directory >>>>"/user/zeas/Arvind"; args[1]=profile
		 * name >>>>"demo_airline_data"; args[2]=""; args[3]= type >>>>"xls";
		 * args[4]=""; args[5]= list of file with : separator >>>> "1"; args[6]=
		 * batchId >>>> "123456"; args[7]=""; args[8]= first record is header
		 * >>>> "true";
		 */
		// args[0]="/user/zeas/Arvind";
		// args[1]="demo_airline_data";
		// //args[2]="";
		// args[3]="xls";
		// args[4]="";
		// args[5]="1";
		// args[6]=args[3]+"_123456";
		// args[7]="";
		// args[8]="true";

		for (int i = 0; i < args.length; i++) {
			System.out.println("arg[" + i + "] =" + args[i]);
		}

		//
		// Log the details of records.
		// ZDPDataAccessObjectImpl accessObjectImpl = new
		// ZDPDataAccessObjectImpl();
		/*
		 * ZDPRunLogDetails runLogDetails = accessObjectImpl
		 * .getLatestRunLogDetail(schedularName, ""); String logFilePath =
		 * runLogDetails.getLogfilelocation();
		 * FileUtility.runLogAppend(logFilePath, "log.txt", logInfo.toString());
		 */
		logInfo = new StringBuilder();
		int status = 1;

		// adding out path details to configuration object.
		conf.set("job.output.path", args[0] + "/" + args[2]); // args[0]
																// /user/zeas/admin/
																// =
																// args[6]=demo_airline_data
																// >>/user/zeas/admin/demo_airline_data
		conf.set("batch.id", args[6]); //
		conf.set("dataSchema.value", args[1]); //
		conf.set("ingestion.time", args[4]); //

		try {
			json = args[9];// "{'name':'demo_airline_data','type':'DataSchema','dataAttribute':[{'Name':'Year','dataType':'int','Primary':'No'},{'Name':'Month','dataType':'int','Primary':'No'},{'Name':'DayofMonth','dataType':'int','Primary':'No'},{'Name':'DayOfWeek','dataType':'int','Primary':'No'},{'Name':'DepTime','dataType':'int','Primary':'No'},{'Name':'CRSDepTime','dataType':'int','Primary':'No'},{'Name':'ArrTime','dataType':'int','Primary':'No'},{'Name':'CRSArrTime','dataType':'int','Primary':'No'},{'Name':'UniqueCarrier','dataType':'string','Primary':'No'},{'Name':'FlightNum','dataType':'int','Primary':'No'},{'Name':'TailNum','dataType':'string','Primary':'No'},{'Name':'ActualElapsedTime','dataType':'int','Primary':'No'},{'Name':'CRSElapsedTime','dataType':'int','Primary':'No'},{'Name':'AirTime','dataType':'int','Primary':'No'},{'Name':'ArrDelay','dataType':'int','Primary':'No'},{'Name':'DepDelay','dataType':'int','Primary':'No'},{'Name':'Origin','dataType':'string','Primary':'No'},{'Name':'Dest','dataType':'string','Primary':'No'},{'Name':'Distance','dataType':'int','Primary':'No'},{'Name':'TaxiIn','dataType':'int','Primary':'No'},{'Name':'TaxiOut','dataType':'int','Primary':'No'},{'Name':'Cancelled','dataType':'int','Primary':'No'},{'Name':'CancellationCode','dataType':'string','Primary':'No'},{'Name':'Diverted','dataType':'int','Primary':'No'},{'Name':'CarrierDelay','dataType':'string','Primary':'No'},{'Name':'WeatherDelay','dataType':'string','Primary':'No'},{'Name':'NASDelay','dataType':'string','Primary':'No'},{'Name':'SecurityDelay','dataType':'string','Primary':'No'},{'Name':'LateAircraftDelay','dataType':'string','Primary':'No'}],'dataSchemaType':'Automatic','fileData':{'fileName':'/user/zeas/admin/inputFile/demo_data_airline_small.csv','fileType':'CSV','format':'CSV','hFlag':'true','mFlag':'false'}}";

			JSONDataParser dataTypeparser = new JSONDataParser();
			System.out.println("JSON.............:" + json);
			JsonColumnValidatorParser attrParser = new JsonColumnValidatorParser();
			Map<Integer, List<ValidationAttribute>> tmpColValidatorMap = attrParser.JsonParser(json);
			Map<Integer, List<ValidationAttribute>> colValidatorMap = attrParser
					.getActualValidatorList(tmpColValidatorMap);

			System.out.println("list of validtor");
			System.out.println("column count" + colValidatorMap.size());
			System.out.println("value:----" + colValidatorMap);

			// used to get column number with data type as a map.
			Map<String, String> columnNameAndDataType = dataTypeparser.JsonParser(json);
			Map<Integer, String> dataType;
			dataType = DataTypeCheckUtility.getcolNumberAndDataTypeMap(columnNameAndDataType);

			// DBUtility.getJSON_DATA(args[1]);
			logInfo.append(zdpLog.INFO("Successfully read json data :\n" + json));
			/*
			 * if(args[3].equalsIgnoreCase("Delimited") ||
			 * args[3].equalsIgnoreCase("Fixed Width")){
			 * json=DBUtility.getJSON_DATA(args[1]+"_Source"); }
			 */
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
			logInfo.append(zdpLog.ERROR("fails to read json data :\n" + json));
			logInfo.append(zdpLog.ERROR("error :\n" + e.toString()));
		}
		// FileUtility.runLogAppend(logFilePath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		conf.set("dataSchema.JSON", json);
		if (args[3].equalsIgnoreCase("xml")) {
			if (json.contains("\"xmlEndTag\"")) {
				String endTag = json.substring(json.indexOf("\"xmlEndTag\":\"") + 13,
						json.indexOf("\"", json.indexOf("\"xmlEndTag\":\"") + 13));
				conf.set("xmlTag.end", "</" + endTag + ">");
			}
		} else if (args[3].equalsIgnoreCase("Delimited")) {
			if (json.contains("\"rowDeli\"") && json.contains("\"rowDeli\"")) {
				String rowDeli = json.substring(json.indexOf("\"rowDeli\":\"") + 11,
						json.indexOf("\"", json.indexOf("\"rowDeli\":\"") + 11));
				String colDeli = json.substring(json.indexOf("\"colDeli\":\"") + 11,
						json.indexOf("\"", json.indexOf("\"colDeli\":\"") + 11));
				conf.set("rowDeli", rowDeli);
				conf.set("colDeli", colDeli);
			}
		} else if (args[3].equalsIgnoreCase("Fixed Width")) {
			if (json.contains("\"noOfColumn\"") && json.contains("\"fixedValues\"")) {
				String noOfColumn = json.substring(json.indexOf("\"noOfColumn\":\"") + 14,
						json.indexOf("\"", json.indexOf("\"noOfColumn\":\"") + 14));
				String fixedValues = json.substring(json.indexOf("\"fixedValues\":\"") + 15,
						json.indexOf("\"", json.indexOf("\"fixedValues\":\"") + 15));
				conf.set("noOfColumn", noOfColumn);
				conf.set("fixedValues", fixedValues);
			}
		}
		// this should be like defined in your yarn-site.xml

		// framework is now "yarn", should be defined like this in
		// mapred-site.xml

		// set mapreduce run mode to yarn

		// like defined in hdfs-site.xml
		/*
		 * conf.set("fs.default.name",
		 * ConfigurationReader.getProperty("HDFS_FQDN")); conf.addResource(new
		 * Path(ConfigurationReader .getProperty("HADOOP_CONF") +
		 * "/yarn-site.xml")); conf.addResource(new Path(ConfigurationReader
		 * .getProperty("HADOOP_CONF") + "/mapred-site.xml"));
		 */

		conf.set("fs.default.name", "hdfs://10.6.185.142:8020");
		conf.addResource(new Path("/etc/hadoop/2.4.2.0-258/0/yarn-site.xml"));
		conf.addResource(new Path("/etc/hadoop/2.4.2.0-258/0/mapred-site.xml"));

		// adding yarn class path to run map reduce in yarn mode.

		String HDP_VERSION = "2.4.2.0-258";// ConfigurationReader.getProperty("HDP_VERSION");
		// Only for HDP *********************************
		if (HDP_VERSION != null && (!HDP_VERSION.isEmpty())) {
			String admincommandopts = conf.get("yarn.app.mapreduce.am.admin-command-opts");
			String commandopts = conf.get("yarn.app.mapreduce.am.command-opts");
			String mapreduceFrameworkPath = conf.get("mapreduce.application.framework.path");
			String mapreduceAppClassPath = conf.get("mapreduce.application.classpath");

			if (admincommandopts.contains("{hdp.version}")) {
				admincommandopts = admincommandopts.replaceAll("\\$\\{hdp.version\\}", HDP_VERSION);
			}
			if (commandopts.contains("{hdp.version}")) {
				commandopts = commandopts.replaceAll("\\$\\{hdp.version\\}", HDP_VERSION);
			}
			if (mapreduceFrameworkPath.contains("{hdp.version}")) {
				mapreduceFrameworkPath = mapreduceFrameworkPath.replaceAll("\\$\\{hdp.version\\}", HDP_VERSION);
			}
			if (mapreduceAppClassPath.contains("{hdp.version}")) {
				mapreduceAppClassPath = mapreduceAppClassPath.replaceAll("\\$\\{hdp.version\\}", HDP_VERSION);
			}
			conf.set("mapreduce.application.framework.path", mapreduceFrameworkPath);
			conf.set("mapreduce.application.classpath", mapreduceAppClassPath);
			conf.set("yarn.app.mapreduce.am.admin-command-opts", admincommandopts);
			conf.set("yarn.app.mapreduce.am.command-opts", commandopts);
		}

		// ***************************

		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.application.classpath", args[7]);
		// added by Deepak to handle First record Header scenario START
		final Boolean ISFRECORDHEADER = Boolean.parseBoolean(args[8]);
		LOG.debug("ISFRECORDHEADER : " + ISFRECORDHEADER);
		conf.setBoolean("first.record.header", ISFRECORDHEADER);
		logInfo.append(zdpLog.INFO("ISFRECORDHEADER: " + ISFRECORDHEADER + " args[8]: " + args[8]));
		// added by Deepak to handle First record Header scenario END
		// Retrieve jar file for class2Add
		// String jar =
		// "/home/hadoop/taphius/databridge-0.0.1-SNAPSHOT/lib/gson-2.2.2.jar";
		// String jar1 =
		// "/home/hadoop/taphius/databridge-0.0.1-SNAPSHOT/lib/oro-2.0.8.jar";
		// String hdfsNameNodeHost =
		// ConfigurationReader.getProperty("HDFS_FQDN");

		String hdfsNameNodeHost = "hdfs://10.6.185.142:8020";
		// conf.set("hadoop.home.dir",
		// ConfigurationReader.getProperty("HADOOP_HOME"));
		// String jar = ConfigurationReader.getProperty("DATABRIDGE_LIB_PATH") +
		// "/gson-2.2.2.jar";
		// String jar1 =
		// ConfigurationReader.getProperty("DATABRIDGE_LIB_PATH")+"/oro-2.0.8.jar";
		// File jarFile = new File(jar);
		// File jarFile1 = new File(jar1);
		// Declare new HDFS location
		// Path hdfsJar = new
		// Path(ConfigurationReader.getProperty("HDFS_USER_PATH") + "/"+
		// jarFile.getName());

		// // Declare new HDFS location
		// Path hdfsJar1 = new
		// Path(ConfigurationReader.getProperty("HDFS_USER_PATH")+"/"
		// + jarFile1.getName());

		// Mount HDFS
		FileSystem hdfs = FileSystem.get(new URI(hdfsNameNodeHost), conf);

		// Copy (override) jar file to HDFS
		// hdfs.copyFromLocalFile(false, true, new Path(jar), hdfsJar);

		// // Copy (override) jar file to HDFS
		// hdfs.copyFromLocalFile(false, true,
		// new Path(jar1), hdfsJar1);

		// Add jar to distributed classPath
		// DistributedCache.addFileToClassPath(hdfsJar, conf);
		// DistributedCache.addFileToClassPath(hdfsJar1, conf);
		/*
		 * Path cleansed = new Path(args[0]+"/cleansed/"); Path quarantine = new
		 * Path(args[0]+"/quarantine/");
		 */
		// Check if frequency is OneTime, if so clean up previous results.
		// Else load data into existing hive table.
		/*
		 * if("onetime".equalsIgnoreCase(args[2])){ //cleanup if any folders
		 * from previous execution cleanUpPreviousResults(hdfs, cleansed,
		 * quarantine ); }
		 */
		System.out.println("\nmap reduce job is initializing....\n");
		logInfo.append(zdpLog.INFO("map reduce job is initializing."));
		String statusInfoForIngestionStarted = "Ingestion Started for " + args[1] + " by" + args[10];
		users.add(args[10]);
		accessObjectActivity.addBulkActivitiesBatchForNewAPI(args[1], statusInfoForIngestionStarted, "ingestion",
				"START", users, args[10]);

		// FileUtility.runLogAppend(logFilePath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		// Job job = new Job(conf);
		CustomHDJob job = null;
		try {
			job = CustomHDJob.getInstance(conf);
			job.setJarByClass(DataIngestionControler.class);
			if (args[3].equalsIgnoreCase(DataValidationConstant.RDBMS)) {
				FileSystem file = FileSystem.get(new URI(args[0]), conf);
				RemoteIterator<LocatedFileStatus> listFiles = file.listFiles(new Path(args[0]), false);
				while (listFiles.hasNext()) {
					LocatedFileStatus fileName = listFiles.next();
					// Ignore _SUCCESS file and directories.

					if ((!fileName.isDirectory()) && fileName.getPath().toString().contains("part")) {
						// System.out.println("files :
						// "+fileName.getPath().toString());
						logInfo.append(zdpLog.INFO("set inputPath for MR:" + fileName));
						FileInputFormat.addInputPath(job, new Path(fileName.getPath().toString()));
					}
				}
			} else {
				/*
				 * if (args[5].length() != 0) { String[] listOfFiles =
				 * args[5].split(":"); for (String fileName : listOfFiles) {
				 * logInfo.append(zdpLog.INFO("set inputPath for MR:" +
				 * fileName)); FileInputFormat.addInputPath(job, new
				 * Path(args[0] + "/" + args[6] + "_" + fileName)); } } else {
				 */
				FileInputFormat.addInputPath(job, new Path(args[0] + args[7]));
				// }
			}
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			FileOutputFormat.setOutputPath(job, new Path(args[0] + args[2]));
			logInfo.append(zdpLog.INFO("set output path for MR:" + args[0] + "/" + args[6]));
			job.setMapperClass(AirportValidationMapper.class);
			// job.setReducerClass(AirportDataValidator.Summer.class);
			logInfo.append(zdpLog.INFO("set mapper class :AirportValidationMapper"));
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			logInfo.append(zdpLog.INFO("set OutputKeyClass :NullWritable"));
			logInfo.append(zdpLog.INFO("set OutputValueClass :Text"));
			logInfo.append(zdpLog.INFO("Input Format type------------->" + args[3]));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			/* begin defaults */
			LOG.info("Format------------->" + args[3]);
			if (args[3].equalsIgnoreCase("xml")) {

				LOG.info("Proccessing XMLINPUTFORMAT................................");
				logInfo.append(zdpLog.INFO("Proccessing XMLINPUTFORMAT................."));
				logInfo.append(zdpLog.INFO("setInputFormatClass XmlInputFormat.class"));
				job.setInputFormatClass(XmlInputFormat.class);
			} else if (args[3].equalsIgnoreCase("json")) {
				logInfo.append(zdpLog.INFO("Proccessing JsonInputFormat................."));
				logInfo.append(zdpLog.INFO("setInputFormatClass JsonInputFormat.class"));
				LOG.info("Proccessing JsonInputFormat................................");
				job.setInputFormatClass(JsonInputFormat.class);
			} else if (args[3].equalsIgnoreCase("Delimited")) {
				logInfo.append(zdpLog.INFO("Proccessing DelimitedFileInputFormat................."));
				logInfo.append(zdpLog.INFO("setInputFormatClass DelimitedFileInputFormat.class"));
				LOG.info("Proccessing DelimitedFileInputFormat................................");
				job.setInputFormatClass(DelimitedFileInputFormat.class);
			} else if (args[3].equalsIgnoreCase("Fixed Width")) {
				logInfo.append(zdpLog.INFO("Proccessing FixedInputFormat................."));
				logInfo.append(zdpLog.INFO("setInputFormatClass FixedInputFormat.class"));
				LOG.info("setInputFormatClass FixedInputFormat................................");
				job.setInputFormatClass(FixedInputFormat.class);

			} else if (args[3].equalsIgnoreCase("xls") || args[3].equalsIgnoreCase("xlsx")) {
				logInfo.append(zdpLog.INFO("Proccessing ExcelInputFormat................."));
				logInfo.append(zdpLog.INFO("setInputFormatClass ExcelInputFormat.class"));
				LOG.info("setInputFormatClass ExcelInputFormat................................");

				job.setMapperClass(ExcelMapper.class);
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);

				job.setInputFormatClass(ExcelInputFormat.class);

			}
			// added to handle local file and RDBMS Ingestion

			else if (args[3].equalsIgnoreCase(DataValidationConstant.RDBMS)) {
				logInfo.append(zdpLog.INFO("Proccessing TextINPUTFORMAT................."));
				LOG.info("Proccessing TextINPUTFORMAT................................");
				logInfo.append(zdpLog.INFO("setInputFormatClass CustomTextInputFormat.class"));
				job.setInputFormatClass(TextInputFormat.class);
			} else {
				logInfo.append(zdpLog.INFO("Proccessing TextINPUTFORMAT................."));
				LOG.info("Proccessing TextINPUTFORMAT................................");
				// added by Deepak to handle First record Header scenario START
				// Bug 78
				logInfo.append(zdpLog.INFO("setInputFormatClass CustomTextInputFormat.class"));
				job.setInputFormatClass(CustomTextInputFormat.class);
				// added by Deepak to handle First record Header scenario end
				// Bug 78
			}
			job.setOutputFormatClass(TextOutputFormat.class);
			logInfo.append(zdpLog.INFO("setOutputFormatClass TextInputFormat.class"));
			/* end defaults */

			job.setNumReduceTasks(0);

			// MultipleOutputs.addNamedOutput(job, "bad",
			// TextOutputFormat.class, NullWritable.class, Text.class);
			// MultipleOutputs.addNamedOutput(job, "quarantine",
			// TextOutputFormat.class, NullWritable.class, Text.class);
			System.out.println("going to submit the job..");
			logInfo.append(zdpLog.INFO("\ngoing to submit the job.."));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			// status=job.waitForCompletion(true)?0:1;

			// SUCCEEDED
			logInfo.append(zdpLog.INFO("MR job submitted ."));
			logInfo.append(zdpLog.INFO("MR job running progress details..."));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			System.out.println("job submiited..............");
			LOG.info("job submiited..............");
			job.submit();
			String jobID = job.getJobID().toString();
			String jobStatus = job.getStatus().getState().toString();
			// IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
			// String actionUserName = ingestionLogDAO.getRunlogInfo(args[1]);
			boolean isInserted = false;
			while (!job.isComplete()) {
				// LOG.info("job submiited222..............");
				String mapProgress = String.format("%.2f", job.mapProgress());
				String reduceProgress = String.format("%.2f", job.reduceProgress());
				if (!(mapPercentage.equalsIgnoreCase(mapProgress))) {
					mapPercentage = mapProgress;
					Float mapValue = Float.parseFloat(mapProgress);
					Float reduceValue = Float.parseFloat(reduceProgress);
					logInfo.append(zdpLog
							.INFO("map progress " + mapValue * 100 + "%    reduce progress " + reduceValue * 100));
					// FileUtility.runLogAppend(logFilePath, "log.txt",
					// logInfo.toString());
					logInfo = new StringBuilder();
				}
				// System.out.println("map progress.. :"+strff);
				jobStatus = job.getStatus().getState().toString();
				if (jobStatus.equalsIgnoreCase("RUNNING")) {
					if (!isInserted) {
						/*
						 * accessObjectImpl.addComponentRunStatus(args[1],
						 * ZDPDaoConstant.INGESTION_ACTIVITY,
						 * ZDPDaoConstant.CHECKING_DATA_QUALITY, jobID,
						 * actionUserName); isInserted = true;
						 */
					}
				}
			}
			jobStatus = job.getStatus().getState().toString();

			System.out.println("jobStatus ===================================================" + jobStatus);
			Log.info("jobStatus ===================================================" + jobStatus);
			switch (jobStatus) {
			case "FAILED":
				/*
				 * accessObjectImpl.addComponentRunStatus(args[1],
				 * ZDPDaoConstant.INGESTION_ACTIVITY,
				 * ZDPDaoConstant.JOB_KILL_FAIL, jobID, actionUserName);
				 */
				System.out.println("jobStatus===Failed");
				String statusInfoForFail = "Ingestion for " + args[1] + " fail";
				accessObjectActivity.addBulkActivitiesBatchForNewAPI(args[1], statusInfoForFail, "ingestion", "Fail",
						users, args[10]);
				break;
			case "KILLED":
				/*
				 * accessObjectImpl.addComponentRunStatus(args[1],
				 * ZDPDaoConstant.INGESTION_ACTIVITY,
				 * ZDPDaoConstant.JOB_TERMINATE, jobID, actionUserName);
				 */
				System.out.println("jobStatus===Killed");

				break;
			case "RUNNING":
				System.out.println("jobStatus===Running");

				break;
			case "SUCCEEDED":
				status = 0;
				/*
				 * accessObjectImpl.addComponentRunStatus(args[1],
				 * ZDPDaoConstant.INGESTION_ACTIVITY,
				 * ZDPDaoConstant.JOB_COMPLETE, jobID, actionUserName);
				 */
				System.out.println("jobStatus===Success");
				String statusInfoForSuccess = "Ingestion for " + args[1] + " success";
				accessObjectActivity.addBulkActivitiesBatchForNewAPI(args[1], statusInfoForSuccess, "ingestion",
						"SUCCESS", users, args[10]);
				break;
			default:
				break;
			}
			LOG.info("job finish..............");

			// generate logs for ingestion
			// logDetails=getLogDetails(job);
			// System.out.println("************* summary of
			// validation*************");
			// System.out.println(logDetails.toString());
			// System.out.println("********************End of
			// summary***************");

			// Counters counters = job.getCounters();
			// Counter cTotal =
			// job.getCounters().findCounter(RECORDS_PROCESSED.TOTAL);
			// Counter cQuarantine =
			// counters.findCounter(RECORDS_PROCESSED.QUARANTINE);
			// long processed =
			// job.getCounters().findCounter(RECORDS_PROCESSED.TOTAL).getValue();
			// long invalid =
			// job.getCounters().findCounter(RECORDS_PROCESSED.QUARANTINE).getValue();
			// float percent = (invalid*100)/processed;
			// records_processed =
			// NumberFormat.getNumberInstance(Locale.US).format(processed)+"
			// records processed, with
			// "+NumberFormat.getNumberInstance(Locale.US).format(invalid)+" (
			// "+percent+" % ) invalid records.";

			if (args[3].equalsIgnoreCase(DataValidationConstant.RDBMS)) {
				FileSystem file = FileSystem.get(new URI(args[0]), conf);
				RemoteIterator<LocatedFileStatus> listOfFiles = file.listFiles(new Path(args[0]), false);
				while (listOfFiles.hasNext()) {
					LocatedFileStatus fileName = listOfFiles.next();
					if (!fileName.isDirectory()) {
						System.out.println("Files :***********" + fileName);
						FileSystem deletFile = FileSystem.get(new URI(fileName.getPath().toString()), conf);
						deletFile.delete(fileName.getPath());
					}
				}
			}

			// read log......
			String logInfo1 = job.getLogs();
			// System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			// System.out.println(logInfo1);
			String er = lpsErr.buf.toString();
			// System.out.println("&&&&&&&&&&&&&&&&&&&&&& error
			// &&&&&&&&&&&&&&&&&&&&&&");
			// System.out.println("----- Log for System.err: -----\n" + er);
			// System.out.println("@@@@@@@@@@@@@@@@@@@ end
			// @@@@@@@@@@@@@@@@@@@");
			logInfo.append(zdpLog.INFO("Map reduce job finished with status:" + status));
			logInfo.append(zdpLog.INFO("**********: Map reduce job execution summary :***********"));
			logInfo.append(zdpLog.INFO(logInfo1));
			if (status == 1)
				logInfo.append(zdpLog.INFO(er));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();

			logInfo.append(zdpLog.INFO("************* summary of validation*************"));
			logDetails = getLogDetails(job);
			logInfo.append(zdpLog.INFO("************End of validation summary***********"));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			// logInfo.append(zdpLog.INFO("**********: Map reduce job execution
			// summary :***********"));
			// System.out.println("************* summary of
			// validation*************");
			// System.out.println(logDetails.toString());
			// System.out.println("********************End of
			// validationsummary***************");

		} catch (Exception ee) {
			// changed throwable to exception and checking if job is null
			String logInfo1 = "";
			if (job != null) {
				logInfo1 = job.getLogs();
			}
			System.out.println("validator exception :" + ee.toString());
			ee.printStackTrace();
			System.out.println("################ error #################");
			System.out.println(logInfo1);
			String er = lpsErr.buf.toString();
			// logInfo.append(zdpLog.INFO("Map reduce job finished with
			// status:"+job.getStatus().getState()));
			logInfo.append(zdpLog.INFO("################ MR JOB Error summary #################"));
			logInfo.append(zdpLog.INFO(logInfo1));
			logInfo.append(zdpLog.ERROR(er));
			// FileUtility .runLogAppend(logFilePath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			System.out.println("----- Log for System.err..: -----\n" + er);
			System.out.println("###############  end ###################");

		}

		//

		return status;
	}

	public static void xls(File inputFile, File outputFile) 
				{
				    // For storing data into CSV files
				    StringBuffer data = new StringBuffer();
				    try 
				    {
				    FileOutputStream fos = new FileOutputStream(outputFile);
				    
				 
				    // Get the workbook object for XLS file
				    XSSFWorkbook  workbook = new XSSFWorkbook (new FileInputStream(inputFile));
				    // Get first sheet from the workbook
		            XSSFSheet sheet = workbook.getSheetAt(0);

				    Cell cell;
				    Row row;
				 
				    // Iterate through each rows from first sheet
				    Iterator<Row> rowIterator = sheet.iterator();
				    while (rowIterator.hasNext()) 
				    {
				            row = rowIterator.next();
				            // For each row, iterate through each columns
				            Iterator<Cell> cellIterator = row.cellIterator();
				            while (cellIterator.hasNext()) 
				            {
				                    cell = cellIterator.next();
				                    
				                    switch (cell.getCellType()) 
				                    {
				                    case Cell.CELL_TYPE_BOOLEAN:
				                            data.append(cell.getBooleanCellValue() + ",");
				                            break;
				                            
				                    case Cell.CELL_TYPE_NUMERIC:
				                            data.append(cell.getNumericCellValue() + ",");
				                            break;
				                            
				                    case Cell.CELL_TYPE_STRING:
				                            data.append(cell.getStringCellValue() + ",");
				                            break;
				 
				                    case Cell.CELL_TYPE_BLANK:
				                            data.append("" + ",");
				                            break;
				                    
				                    default:
				                            data.append(cell + ",");
				                    }
				                    
				                    
				            }
				            data.append('\n'); 
				    }
				 
				    fos.write(data.toString().getBytes());
				    fos.close();
				    }
				    catch (FileNotFoundException e) 
				    {
				            e.printStackTrace();
				    }
				    catch (IOException e) 
				    {
				            e.printStackTrace();
				    }
				    }

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException {
		ToolRunner.run(new DataIngestionControler(""), args);
		/*File input = new File("C:\\Users\\25429\\Desktop\\MRP_Variation_Report 2016-09-14.xlsx");
		File output = new File("C:\\Users\\25429\\Desktop\\cvggf.csv");

		xls(input,output);*/
		
	}

	private void cleanUpPreviousResults(FileSystem fs, Path cleanse, Path quarantine) {
		try {
			if (fs.exists(cleanse))
				fs.delete(cleanse, true);
			if (fs.exists(quarantine))
				fs.delete(quarantine, true);
		} catch (IOException e) {
			LOG.error("Error cleaning up previous execution path at HDFS");
		}
	}

	public String getRecordsProcessed() {
		return records_processed;
	}

	public String getSchemaJson() {
		return json;
	}

	public IngestionLogDetails getLogDetails() {

		return logDetails;
	}

	public String getLogInfo() {

		return logInfo.toString();
	}

	private IngestionLogDetails getLogDetails(Job job) {

		IngestionLogDetails logDetails = new IngestionLogDetails();
		ValidationLogDetails validationLogDetails = new ValidationLogDetails();
		logDetails.setValidationLogDetails(validationLogDetails);
		StringBuilder validationSummary = new StringBuilder();
		StringBuilder qurantineSummary = new StringBuilder();
		Long quarantileRecords = 0l;
		for (RECORDS_PROCESSED processed : RECORDS_PROCESSED.values()) {

			try {
				String type = processed.toString();
				Long record = job.getCounters().findCounter(processed).getValue();
				// System.out.println("type :"+type +" record:"+record);
				switch (type) {

				case "TOTAL":
					logDetails.setNoOfRecords(record);
					logDetails.getValidationLogDetails().setNoOfTotalRecords(record);
					validationSummary.append("Total noOfRecord :" + record + "\n");
					break;

				case "CLEANSED":
					logDetails.setNoOfCleansed(record);
					validationSummary.append("Total setNoOfCleansed record :" + record + "\n");
					break;

				case "noOfRangeFails":
					logDetails.getValidationLogDetails().setNoOfRangeFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfRangeFails record :" + record + "\n");
					break;

				case "noOfRegexFails":
					logDetails.getValidationLogDetails().setNoOfRegexFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfRegexFails record :" + record + "\n");
					break;

				case "noOfFixedlLengthFails":
					logDetails.getValidationLogDetails().setNoOfFixedlLengthFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfFixedlLengthFails record :" + record + "\n");
					break;

				case "noOfWhiteListFails":
					logDetails.getValidationLogDetails().setNoOfWhiteListFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfWhiteListFails record :" + record + "\n");
					break;

				case "noOfBlackListFails":
					logDetails.getValidationLogDetails().setNoOfBlackListFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfBlackListFails record :" + record + "\n");
					break;

				case "noOfMandatoryFails":
					logDetails.getValidationLogDetails().setNoOfMandatoryFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfMandatoryFails record :" + record + "\n");
					break;

				case "noOfDataTypeMismatchFails":
					logDetails.getValidationLogDetails().setNoOfDataTypeMismatchFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfDataTypeMismatchFails record :" + record + "\n");
					break;

				case "noOfColumnMismatchFails":
					logDetails.getValidationLogDetails().setNoOfColumnMismatchFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfColumnMismatchFails record :" + record + "\n");
					break;

				case "noOfotherFails":
					logDetails.getValidationLogDetails().setNoOfotherFails(record);
					quarantileRecords = quarantileRecords + record;
					qurantineSummary.append("Total noOfotherFails record :" + record + "\n");
					break;
				}
			} catch (IOException e) {

			}
		}
		logInfo.append(validationSummary.toString() + "\n");
		logInfo.append("No of Quarantine record :" + quarantileRecords + "\n");
		logInfo.append("::::::::: Quarantine summary::::::::::\n");
		logInfo.append(qurantineSummary.toString() + "\n");
		// System.out.println("validation :"+validationLogDetails.toString());
		// System.out.println("lllllllll\n"+logDetails.getValidationLogDetails().toString());
		return logDetails;
	}

}

/*
 * class LoggedPrintStream extends PrintStream { final StringBuilder buf; final
 * PrintStream underlying; LoggedPrintStream(StringBuilder sb, OutputStream os,
 * PrintStream ul) { super(os); this.buf = sb; this.underlying = ul; } public
 * static LoggedPrintStream create(PrintStream toLog) { try { final
 * StringBuilder sb = new StringBuilder(); Field f =
 * FilterOutputStream.class.getDeclaredField("out"); f.setAccessible(true);
 * OutputStream psout = (OutputStream) f.get(toLog); return new
 * LoggedPrintStream(sb, new FilterOutputStream(psout) { public void write(int
 * b) throws IOException { super.write(b); sb.append((char) b); } }, toLog); }
 * catch (NoSuchFieldException shouldNotHappen) { } catch
 * (IllegalArgumentException shouldNotHappen) { } catch (IllegalAccessException
 * shouldNotHappen) { } return null; } }
 */