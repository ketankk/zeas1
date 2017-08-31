package com.taphius.dataloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.taphius.databridge.model.SchemaAttributes;
import com.taphius.databridge.utility.FileUtility;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.databridge.utility.ZDPLog;
import com.taphius.pipeline.HiveClient;
import com.taphius.validation.mr.AirportDataValidator;
import com.taphius.validation.mr.IngestionLogDetails;
import com.itc.zeas.project.model.ProjectEntity;
import com.zdp.dao.SearchCriteriaEnum;
import com.itc.zeas.project.model.SearchCriterion;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.ingestion.model.ZDPRunLogDetails;
import com.itc.zeas.project.extras.ZDPDaoConstant;

/** Little example which copies a locally mounted dir to hdfs. */

public class DataLoader {

	private static Logger LOG = Logger.getLogger(DataLoader.class.getName());
	private StringBuilder existFiles;
	private boolean isHadoopFail = false;
	public static ZDPLog zdpLog = ZDPLog.getZDPLog("DataLoader");

	/**
	 * Minimum delay before starting file copy to HDFS.
	 */
	private static final long DELAY = 1;

	public static final String ARCHIVE_DIR = "archive-dir";
	public static final String BATCH_ID = "batch-id";
	public static final String BATCH_FREQUENCY = "batchFrequency";
	public static final String INGESTION_ID = "ingestion-id";
	public static final String DEFAULT_HOST = "hdfs://ec2-54-210-74-58.compute-1.amazonaws.com:9000";
	public static final String NAMENODE_HOST = "namenode";
	public static final String PROP_FILE = "/example.properties";
	public static final String SOURCE_DIR = "source-dir";
	public static final String SCHEMA = "schemaName";
	public static final String DATASET = "datasetName";
	public static final String HDFS_TARGET = "targetHDFSPath";
	public static final String SCHEDULE_TIME = "scheduleTime";
	public static final String FILE_FORMAT = "fileFormat";
	public static final String FILE_TYPE = "fileType";
	public static final String SCHEDULAR_NAME = "schedularName";
	public static final String KEY_PROVIDER_URI = "dfs.encryption.key.provider.uri";

	// added by Deepak to handle First record Header scenario
	public static final String IS_FIRST_RECORD_HEADER = "first.record.header";

	private IngestionLogDAO ingestionLogDAO;
	private IngestionLogDetails logDetails;
	private String schedularName = "";
	private StringBuilder logInfo = null;
	private String logFilepath = "";

	/**
	 * Batch id for the current ingestion run
	 */
	private String batchId;

	/** Callable class. Each thread copies specified files to hdfs. */
	private class DataLoaderCallable implements Callable<Integer> {
		private Configuration conf;
		private File source;

		public DataLoaderCallable(Configuration conf, File f) {
			this.source = f;
			this.conf = conf;
		}

		/** Copies file to dfs - then moves the copied file to archive batch. */
		public Integer call() throws ZeasException {
			int copyStatus = copyFile(source, conf);
			if (copyStatus == 0) {
				String archiveLoc = conf.get(ARCHIVE_DIR) + "/"
						+ conf.get(BATCH_ID);
				File archiveDir = new File(archiveLoc);
				String fileName = source.getName();

				if (archiveDir.exists() && archiveDir.isDirectory()) {
					LOG.info("archive directory exists " + archiveLoc);
					String dest = archiveDir + "/" + fileName;
					source.renameTo(new File(dest));
				} else {
					LOG.info("archive directory created " + archiveLoc);
					String dest = archiveDir + "/" + fileName;
					archiveDir.mkdirs();
					source.renameTo(new File(dest));
				}
			}
			return copyStatus;
		}

		/** Utility to copy file from local to hadoop file system. */
		private int copyFile(File source, Configuration conf) throws ZeasException {
			FileSystem fs;
			Path fromLocal = null;
			try {
				String host = conf.get(NAMENODE_HOST);
				conf.set("hadoop.home.dir",
						ConfigurationReader.getProperty("HADOOP_HOME"));
				/**
				 * If transparent encryption is enabled on the cluster,
				 * We need to specify Key Provider Uri
				 */
				if(ConfigurationReader.getProperty("KEYPROVIDER_URI") != null){
					conf.set(KEY_PROVIDER_URI, ConfigurationReader.getProperty("KEYPROVIDER_URI"));
				}
				fs = FileSystem.get(new URI(host), conf);
				fromLocal = new Path(source.getCanonicalPath());
				Path toHdfs = new Path(host + conf.get(HDFS_TARGET) + "/"
						+ conf.get(BATCH_ID) + "_" + source.getName());
				LOG.debug("from copyFiles " + source.getCanonicalPath()
						+ "to HDFS - " + toHdfs.toString());
				if (!fs.exists(toHdfs)) {
					fs.copyFromLocalFile(fromLocal, toHdfs);
					LOG.info("file copied from " + source.getCanonicalPath()
							+ "to HDFS - " + toHdfs.toString()
							+ " successfully.");
					logInfo.append(zdpLog.INFO("file copied from "
							+ source.getCanonicalPath() + "to HDFS - "
							+ toHdfs.toString() + " successfully."));
					FileUtility.runLogAppend(logFilepath, "log.txt",
							logInfo.toString());
					logInfo = new StringBuilder();
					return 0;
				} else {
					LOG.error("-----File is already exist for particular DataSet----------");
					String localFile = fromLocal.getName().toString().trim();
					logInfo.append(zdpLog
							.DEBUG("-----File is already exist for this DataSet----------"
									+ localFile));
					FileUtility.runLogAppend(logFilepath, "log.txt",
							logInfo.toString());
					logInfo = new StringBuilder();
					existFiles.append(localFile + ",");
					return -1;
				}
			} catch (IOException | URISyntaxException e) {
				LOG.error(e.getMessage());
				isHadoopFail = true;
				LOG.error("-----Ingestion not performed. Due to Hadoop issue.....----------");
				String localFile = "";
				// avoiding nullpointer exception
				if (fromLocal != null) {
					localFile = fromLocal.getName().toString().trim();
				}
				logInfo.append(zdpLog
						.ERROR("-----Ingestion not performed. Due to Hadoop issue.....----------"
								+ localFile));
				FileUtility.runLogAppend(logFilepath, "log.txt",
						logInfo.toString());
				logInfo = new StringBuilder();
				existFiles.append(localFile + ",");
				logDetails
						.setIngestionFails("Ingestion not performed. Due to Hadoop issue.....|"
								+ new Timestamp(System.currentTimeMillis()));
				ingestionLogDAO.updateLogObject(
						Integer.parseInt(conf.get(INGESTION_ID)),
						conf.get(BATCH_ID), "Ingestion", "Failed", logDetails);
				return -2;
			}
		}
	}

	/**
	 * FileChecker main thread will call this function with prop file created
	 * from DB entry.
	 * 
	 * @throws Exception
	 */
	public void run(final Properties prop, String sourceDir) throws ZeasException {

		ingestionLogDAO = new IngestionLogDAO();
		logDetails = new IngestionLogDetails();
		logInfo = new StringBuilder();
		logInfo.append(zdpLog.INFO("data bridge  application is up."));

		existFiles = new StringBuilder();
		schedularName = prop.getProperty(SCHEDULAR_NAME);
		System.out.println("schedularName :" + schedularName);
		// Log the details of records.
		ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();
		try {
			// System.out.println("getLatestRunLogDetail :"+schedularName);
			ZDPRunLogDetails runLogDetails = accessObjectImpl
					.getLatestRunLogDetail(schedularName, "");
			logFilepath = runLogDetails.getLogfilelocation();
		} catch (Exception e1) {
			LOG.info("Error found to read :" + e1.toString());
			logInfo.append(zdpLog
					.ERROR("Error during reading the logfile path from database:"
							+ e1.toString()));
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
			e1.printStackTrace();
		}
		batchId = LoaderUtil.getBatchID(prop.getProperty(BATCH_FREQUENCY));
		Timestamp ingestionTime = LoaderUtil.getIngestionTime();
		StringBuilder listHDFSFiles = new StringBuilder();
		final StringBuilder failedFiles = new StringBuilder();
		LOG.info("Trying to ingest data from path : " + sourceDir);
		logInfo.append(zdpLog.INFO("ingesting data from source path : "
				+ sourceDir));
		// System.out.println("logfile path:"+logFilepath);
		FileUtility.runLogAppend(logFilepath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		// String batchId = prop.getProperty(BATCH_ID);
		final String schema = prop.getProperty(SCHEMA);
		File file = new File(sourceDir);
		File[] listOfFiles = null;
		FilenameFilter fileNameFilter = new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (!new File(name).isDirectory()) {
					if (name.equalsIgnoreCase("_DONE")) {
						return false;
					} else if (name.equalsIgnoreCase("archive")) {
						return false;
					} else if (!(prop.getProperty(FILE_TYPE).equalsIgnoreCase(
							"Delimited") || prop.getProperty(FILE_TYPE)
							.equalsIgnoreCase("Fixed Width"))) {
						String fileFormat = prop.getProperty(FILE_FORMAT);
						if (fileFormat.equalsIgnoreCase("xlsx")
								|| fileFormat.equalsIgnoreCase("xls")) {
							fileFormat = "csv";
						}
						if ((!name.endsWith(fileFormat.toLowerCase()))) {
							LOG.info("Corresponding schema "
									+ schema
									+ " is not matching with file type-------> "
									+ name);
							logInfo.append(zdpLog
									.INFO("Corresponding schema "
											+ schema
											+ " is not matching with file type-------> "
											+ name));
							logInfo.append(zdpLog
									.INFO("file doesn't pick for ingestion:"
											+ name));
							FileUtility.runLogAppend(logFilepath, "log.txt",
									logInfo.toString());
							logInfo = new StringBuilder();
							if (!(name.endsWith("xls") || name.endsWith("xlsx")))
								failedFiles.append(name + ",");
							return false;
						}
					}
				}
				logInfo.append(zdpLog.INFO("file picked for ingestion:" + name));
				FileUtility.runLogAppend(logFilepath, "log.txt",
						logInfo.toString());
				logInfo = new StringBuilder();
				return true;
			}
		};
		if (file.isDirectory()) {
			listOfFiles = file.listFiles(fileNameFilter);
		}
		if (listOfFiles == null || listOfFiles.length == 0) {

			try {
				// add ingestion when there is no file
				if (failedFiles.length() == 0) {
					LOG.info("no files found " + sourceDir);
					logInfo.append(zdpLog
							.INFO("Ingestion not performed.There are no files to process. "));
					logDetails
							.setIngestionFails(zdpLog
									.INFO("Ingestion not performed.There are no files to process ."));
					ingestionLogDAO.addLogObject(
							Integer.parseInt(prop.getProperty(INGESTION_ID)),
							batchId, "Ingestion", "Failed", logDetails);
					// System.out.println("complete Status"+completeStatus);
					FileUtility.runLogAppend(logFilepath, "log.txt",
							logInfo.toString());
					logInfo = new StringBuilder();
					return;
				}

			} catch (NumberFormatException e) {
				LOG.error(e.toString());
				logInfo.append(zdpLog.ERROR("Error :" + e.toString()));
				FileUtility.runLogAppend(logFilepath, "log.txt",
						logInfo.toString());
				logInfo = new StringBuilder();
			}
		}
		ScheduledExecutorService scheduledExecutorService = Executors
				.newScheduledThreadPool(listOfFiles.length);
		Set<Future<Integer>> set = new HashSet<Future<Integer>>();

		Configuration conf = new Configuration();
		conf.set(
				ARCHIVE_DIR,
				prop.getProperty(ARCHIVE_DIR, prop.getProperty(SOURCE_DIR)
						+ "/archive"));
		conf.set(SCHEDULAR_NAME, prop.getProperty(SCHEDULAR_NAME));
		conf.set(BATCH_ID, this.batchId);
		conf.set(BATCH_FREQUENCY, prop.getProperty(BATCH_FREQUENCY));
		conf.set(INGESTION_ID, prop.getProperty(INGESTION_ID));
		conf.set(NAMENODE_HOST, prop.getProperty(NAMENODE_HOST, DEFAULT_HOST));
		conf.set(SOURCE_DIR, prop.getProperty(SOURCE_DIR));
		conf.set(SCHEMA, prop.getProperty(SCHEMA));
		conf.set(DATASET, prop.getProperty(DATASET));
		conf.set(HDFS_TARGET, prop.getProperty(HDFS_TARGET));
		ScheduledFuture<Integer> future = null;
		logInfo.append(zdpLog.INFO("Configuration summary for ingestion."));
		logInfo.append(zdpLog.INFO("ARCHIVE_DIR:"
				+ prop.getProperty(SOURCE_DIR) + "/archive"));
		logInfo.append(zdpLog.INFO("SCHEDULAR_NAME:"
				+ prop.getProperty(SCHEDULAR_NAME)));
		logInfo.append(zdpLog.INFO("BATCH_ID:" + this.batchId));
		logInfo.append(zdpLog.INFO("INGESTION_ID:"
				+ prop.getProperty(INGESTION_ID)));
		logInfo.append(zdpLog.INFO("SOURCE_DIR:" + prop.getProperty(SOURCE_DIR)));
		logInfo.append(zdpLog.INFO("SCHEMA:" + prop.getProperty(SCHEMA)));
		logInfo.append(zdpLog.INFO("DATASET:" + prop.getProperty(DATASET)));
		logInfo.append(zdpLog.INFO("HDFS_TARGET:"
				+ prop.getProperty(HDFS_TARGET)));
		logInfo.append(zdpLog.INFO("*************end****************"));
		FileUtility.runLogAppend(logFilepath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		// conf.addResource(new
		// Path(ConfigurationReader.getProperty("HADOOP_HOME") +
		// "/etc/hadoop/core-site.xml"));

		// activites
		ZDPDataAccessObjectImpl accessObjectActivity = new ZDPDataAccessObjectImpl();
		String ingestionStatus = "";
		ProjectEntity ingestionRunProjectEntity = null;
		String profileName = prop.getProperty(SCHEMA);
		String actionUserName = ingestionLogDAO.getRunlogInfo(profileName);
		List<String> users = new ArrayList<>();
		String statusInfo = "";

		// add ingestion started...
		logDetails.setIngestionStart("Ingestion start  |"
				+ new Timestamp(System.currentTimeMillis()));
		logInfo.append(zdpLog.INFO("Ingestion initiated..."));
		FileUtility.runLogAppend(logFilepath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		ingestionLogDAO.addLogObject(
				Integer.parseInt(prop.getProperty(INGESTION_ID)), batchId,
				"Ingestion", "Started", logDetails);
		ingestionStatus = "started";

		try {
			List<SearchCriterion> ingestionRunList = new ArrayList<>();
			SearchCriterion schedular = new SearchCriterion("name",
					prop.getProperty(SCHEDULAR_NAME), SearchCriteriaEnum.EQUALS);
			ingestionRunList.add(schedular);
			ingestionRunProjectEntity = accessObjectActivity.findExactObject(
					ZDPDaoConstant.ZDP_ENTITY_TABLE, ingestionRunList);
			Long schedularId = 0l;
			if (ingestionRunProjectEntity != null && ingestionRunProjectEntity.getId() > 0) {
				schedularId = ingestionRunProjectEntity.getId();
				users = accessObjectActivity.getUserListForGivenId(
						schedularId.toString(), ZDPDaoConstant.ZDP_INGESTION);
				if (!users.contains(actionUserName)) {
					users.add(actionUserName);
				}
			}

		} catch (Exception e) {
			System.out.println(e.toString());
		}

		// end
		List<String> duplicateFiles = new ArrayList<>();
		boolean isInserted = false;
		for (File f : listOfFiles) {
			LOG.info("calling callable for file..." + f.toString());
			// checking md5 value for file
			String schemaName = prop.getProperty(SCHEMA);
			String md5 = LoaderUtil.getMD5(f);
			ZDPDataAccessObjectImpl accessObject = new ZDPDataAccessObjectImpl();
			SearchCriterion criterion = new SearchCriterion("schemaname",
					schemaName, SearchCriteriaEnum.EQUALS);
			SearchCriterion criterion1 = new SearchCriterion("md5", md5,
					SearchCriteriaEnum.EQUALS);
			List<SearchCriterion> criteriaList = new ArrayList<>();
			criteriaList.add(criterion);
			criteriaList.add(criterion1);
			ProjectEntity ingestionInfoProjectEntity = null;
			try {
				ingestionInfoProjectEntity = accessObject.findExactObject(
						ZDPDaoConstant.ZDP_INGESTION_RUN_INFO, criteriaList);
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (ingestionInfoProjectEntity == null
					|| (ingestionInfoProjectEntity != null && ingestionInfoProjectEntity
							.getId() == 0)) {
				ProjectEntity projectEntity = new ProjectEntity();
				projectEntity.setSchemaname(schemaName);
				projectEntity.setMd5(md5);
				// entity.setCreatedBy("user");
				projectEntity.setFilename(f.getName());
				projectEntity.setSchemaType(ZDPDaoConstant.ZDP_INGESTION_RUN_INFO);
				ProjectEntity addProjectEntity = accessObject.addEntity(projectEntity);
				if (addProjectEntity != null) {
					System.out.println("run_info_created :"
							+ addProjectEntity.getSchemaname() + " :id"
							+ addProjectEntity.getId());
				}
			} else {
				String existMD5 = ingestionInfoProjectEntity.getMd5();
				if (existMD5.equals(md5)) {
					duplicateFiles.add(f.getName());
					continue;
				}
			}
			// end md5
			Callable<Integer> callable = new DataLoaderCallable(conf, f);
			future = scheduledExecutorService.schedule(callable, DELAY,
					TimeUnit.SECONDS);
			// set to verify copying is complete and valid
			listHDFSFiles.append(f.getName().toString().trim() + ":");
			set.add(future);
			try {
				if (!isInserted) {
					accessObjectImpl.addComponentRunStatus(schemaName,
							ZDPDaoConstant.INGESTION_ACTIVITY,
							ZDPDaoConstant.FILE_COPYING, "UNDEFINED",
							actionUserName);
					isInserted = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (listHDFSFiles.length() > 0) {
			listHDFSFiles.delete((listHDFSFiles.length() - 1),
					listHDFSFiles.length());
		}
		boolean status = LoaderUtil.verifyFileTransferStatus(set) ? true
				: false;

		LOG.info("Completed file transfer to HDFS ..");
		// End of fileCopy Process cleaning _DONE file.
		File[] listFiles = file.listFiles();
		for (File f : listFiles) {
			if (f.toString().endsWith("_DONE")) {
				LOG.info("Cleaning up the _DONE file from source location");
				f.delete();
				break;
			}
		}

		scheduledExecutorService.shutdown();
		// Counting number of failed files in ingestion and updating log
		// information.*****************

		// total number of files list
		StringBuilder files = new StringBuilder();
		for (File f : listFiles) {
			if (f.isFile())
				files.append(" ," + f.getName());
		}
		// end
		if (duplicateFiles.size() != 0 || existFiles.length() != 0
				|| failedFiles.length() != 0) {
			ingestionStatus = "fail";
			logDetails.setIngestionComplete("Ingestion failed ~"
					+ LoaderUtil.getDetailedMessege(listOfFiles,
							existFiles.toString(), failedFiles.toString(),
							duplicateFiles) + " |"
					+ new Timestamp(System.currentTimeMillis()));
			ingestionLogDAO.updateLogObject(
					Integer.parseInt(prop.getProperty(INGESTION_ID)), batchId,
					"Ingestion", "Completed", logDetails);
			LOG.error("User " + actionUserName + ": Ingestion failed for '"
					+ profileName + ". IngestionType: file");
			logInfo.append(zdpLog
					.DEBUG("Ingestion failed summary information for profileName"
							+ profileName + ". IngestionType: file"));
			logInfo.append(zdpLog.INFO("Total number of files for ingestion:"
					+ files.toString()));
			if (duplicateFiles.size() > 0) {
				logInfo.append(zdpLog
						.INFO("Total number of duplicates files for ingestion:"
								+ duplicateFiles.toString()));
			}
			if (failedFiles.length() > 0) {
				logInfo.append("total number of mismatch file type for ingestion:"
						+ failedFiles.toString() + " \n");
			}
			if (existFiles.length() > 0) {
				logInfo.append(zdpLog
						.INFO("total number of failed file type for ingestion due to exist or other issues:"
								+ existFiles.toString()));
			}
			logInfo.append(zdpLog
					.DEBUG("end of Ingestion failed summary information "));
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
		} else {
			ingestionStatus = "success";
			logDetails.setIngestionComplete("Ingestion complete ~"
					+ LoaderUtil.getDetailedMessege(listOfFiles,
							existFiles.toString(), failedFiles.toString(),
							duplicateFiles) + " |"
					+ new Timestamp(System.currentTimeMillis()));
			ingestionLogDAO.updateLogObject(
					Integer.parseInt(prop.getProperty(INGESTION_ID)), batchId,
					"Ingestion", "Completed", logDetails);
			LOG.info("User " + actionUserName
					+ ": Ingestion completed for '" + profileName
					+ ". IngestionType: file");

			logInfo.append(zdpLog
					.INFO("Ingestion completed summary information for profileName"
							+ profileName + ". IngestionType: file"));
			logInfo.append(zdpLog.INFO("total number of files for ingestion:"
					+ files.toString()));
			if (duplicateFiles.size() > 0) {
				logInfo.append(zdpLog
						.DEBUG("total number of duplicates files for ingestion:"
								+ duplicateFiles.toString()));
			}
			if (failedFiles.length() > 0) {
				logInfo.append(zdpLog
						.DEBUG("total number of mismatch file type for ingestion:"
								+ failedFiles.toString()));
			}
			if (existFiles.length() > 0) {
				logInfo.append(zdpLog
						.DEBUG("total number of failed file type for ingestion due to exist or other issues:"
								+ existFiles.toString()));
			}
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
		}

		// end**************************************
		if (ingestionStatus.equalsIgnoreCase("fail")) {
			if (users != null && users.size() > 0) {
				statusInfo = "Ingestion for '" + profileName + "' "
						+ ingestionStatus;
				System.out.println("***********************************"
						+ statusInfo);
				try {
					accessObjectActivity.addActivitiesBatchForNewAPI(
							profileName, statusInfo,
							ZDPDaoConstant.INGESTION_ACTIVITY,
							ingestionStatus.toUpperCase(), users,
							actionUserName);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		if (listHDFSFiles.length() > 0 && ((!isHadoopFail) || status)) {
			String yarnClassPaths = "";
			String[] args1 = new String[2];
			args1[0] = ShellScriptExecutor.BASH;
			args1[1] = System.getProperty("user.home")
					+ "/zeas/Config/getYarnClassPath.sh";

			ProcessBuilder pb = new ProcessBuilder(args1);
			pb.redirectErrorStream(true);
			Process p = null;
			String text;
			try {
				p = pb.start();
				BufferedReader br = new BufferedReader(new InputStreamReader(
						p.getInputStream()));
				text = br.readLine();
				if (text != null) {
					yarnClassPaths = text;
				}
			} catch (IOException e) {
			}

			String[] args = new String[9];
			args[0] = prop.getProperty(HDFS_TARGET);
			args[1] = prop.getProperty(SCHEMA);
			args[2] = "onetime";
			if (prop.getProperty(FILE_TYPE) != null
					&& (prop.getProperty(FILE_TYPE).equalsIgnoreCase(
							"Delimited") || prop.getProperty(FILE_TYPE)
							.equalsIgnoreCase("Fixed Width"))) {
				args[3] = prop.getProperty(FILE_TYPE);
			} else {
				args[3] = prop.getProperty(FILE_FORMAT);
			}
			args[4] = ingestionTime.toString();
			args[5] = listHDFSFiles.toString();
			args[6] = batchId;
			args[7] = yarnClassPaths;
			// added by Deepak to handle First record Header scenario
			args[8] = prop.getProperty(IS_FIRST_RECORD_HEADER);
			LOG.debug("IS_FIRST_RECORD_HEADER: "+IS_FIRST_RECORD_HEADER);
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
			runValidationRules("", args, prop.getProperty(DATASET),
					prop.getProperty(INGESTION_ID), batchId, logDetails,
					ingestionLogDAO, profileName, users, schedularName,
					logFilepath, actionUserName);
		}
	}

	/**
	 * Method for running validation
	 * 
	 * @param columnNameAndDataType
	 * @param profileName
	 * @param users

	 **/
	public void runValidationRules(String columnNameAndDataType, String[] args,
			String dataSet, String ingestionId, String batchId,
			IngestionLogDetails logDetails, IngestionLogDAO ingestionLogDAO,
			String profileName, List<String> users, String schdlrName,
			String logPath, String userName) throws ZeasException {
		ZDPDataAccessObjectImpl accessObjectActivity = new ZDPDataAccessObjectImpl();
		String ingestionStatus = "";
		args[0] = ConfigurationReader.getProperty("HDFS_FQDN") + args[0];
		schedularName = schdlrName;
		logFilepath = logPath;
		try {
			// FileUtility.runLogAppend(logFilepath, "log.txt",
			// logInfo.toString());
			logInfo = new StringBuilder();
			// log details of validation start time.
			logDetails.setValidationStart("Validation start  | "
					+ new Timestamp(System.currentTimeMillis()));
			ingestionLogDAO.updateLogObject(Integer.parseInt(ingestionId),
					batchId, "Validation", "Started", logDetails);
			logInfo.append(zdpLog.INFO("Validation initialize."));
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
			// Call map reduce job for running validations
			AirportDataValidator validator = new AirportDataValidator(
					schedularName);
			// Log the details of records.
			// ZDPDataAccessObjectImpl accessObjectImpl=new
			// ZDPDataAccessObjectImpl();
			// ZDPRunLogDetails
			// runLogDetails=accessObjectImpl.getLatestRunLogDetail(schedularName,
			// "");
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
LOG.info("before if else condition");
			if (ToolRunner.run(validator, args) != 0) {
				
				System.out.println("inside if condition ToolRunner");
				logDetails.setValidationComplete("Validation failed   | "
						+ new Timestamp(System.currentTimeMillis()));
				logInfo.append(zdpLog.DEBUG("Mapreduce Job failed. "
						+ "Validation failed   ."));
				ingestionLogDAO.updateLogObject(Integer.parseInt(ingestionId),
						batchId, "Validation", "Failed", logDetails);
				FileUtility.runLogAppend(logFilepath, "log.txt",
						logInfo.toString());
				logInfo = new StringBuilder();
				ingestionStatus = "fail";
			} else {
				System.out.println("inside else condition ToolRunner");

				long eValidationTime = System.currentTimeMillis();
				IngestionLogDetails templog = validator.getLogDetails();
				if (templog != null) {
					logDetails.setNoOfRecords(templog.getNoOfRecords());
					logDetails.setNoOfCleansed(templog.getNoOfCleansed());
					logDetails.setValidationLogDetails(templog
							.getValidationLogDetails());
				}
				logDetails.setValidationComplete("Validation complete  |"
						+ new Timestamp(eValidationTime));
				ingestionLogDAO.updateLogObject(Integer.parseInt(ingestionId),
						batchId, "Validation", "Completed", logDetails);
				ingestionStatus = "success";
				logInfo.append(zdpLog.INFO("Validation complete "));
			}
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();

			// Register dataset and qualentine

			DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(
					DataSchema.class);
			DataSchema schema = parser.getDSConfigDetails(validator
					.getSchemaJson());
			columnNameAndDataType = HiveClient.getSchemaAttributes(
					schema.getDataAttribute()).toString()
					+ ",ingestionTime timestamp" + ",sourceFile string";
			registerDataset(columnNameAndDataType, validator.getSchemaJson(),
					dataSet, args[0], batchId);
			registerQuarantine(dataSet + "_quarantine", args[0], batchId);

			// Catching exceptions and logging details
		} catch (Exception e) {
			ingestionStatus = "fail";
			try {
				accessObjectActivity.addComponentRunStatus(profileName,
						ZDPDaoConstant.INGESTION_ACTIVITY,
						ZDPDaoConstant.JOB_KILL_FAIL, "UNDEFINED", userName);
			} catch (SQLException e1) {
			}
			logDetails.setValidationComplete("Validation failed   | "
					+ new Timestamp(System.currentTimeMillis()));
			ingestionLogDAO.updateLogObject(Integer.parseInt(ingestionId),
					batchId, "Validation", "Failed", logDetails);
			logInfo.append(zdpLog.ERROR("Exception Occured. "
					+ "Validation failed  ."));
			logInfo.append(zdpLog.ERROR("Exception :-" + e.toString()));
			FileUtility
					.runLogAppend(logFilepath, "log.txt", logInfo.toString());
			logInfo = new StringBuilder();
			e.printStackTrace();
		} finally {
			if (users != null && users.size() > 0) {
				String statusInfo = "Ingestion for '" + profileName + "' "
						+ ingestionStatus;
				try {
					accessObjectActivity.addActivitiesBatchForNewAPI(
							profileName, statusInfo,
							ZDPDaoConstant.INGESTION_ACTIVITY,
							ingestionStatus.toUpperCase(), users, userName);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/** For use only by the example client (main). */
	private void readPropertiesFile(Properties prop, String PROP_FILE)
			throws IOException {
		InputStream is = getClass().getResourceAsStream(PROP_FILE);
		prop.load(is);
		is.close();
	}

	private void registerDataset(String columnNameAndDataType,
			String schemaStr, String tableName, String dataPath, String batchId) throws ZeasException {

		if (dataPath.charAt(dataPath.length() - 1) != '/') {
			dataPath = dataPath + "/";
		}

		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home")
				+ "/zeas/Config/createHiveTable.sh";
		args[2] = tableName;
		args[3] = columnNameAndDataType;
		args[4] = dataPath + "cleansed";
		args[5] = dataPath;
		args[6] = dataPath + batchId;
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
		
		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);
		
		registerParquet(columnNameAndDataType, schemaStr, tableName, dataPath, batchId);
		registerView(schemaStr, tableName);
		/*
		 * try { hclient.createTable(schema.getDataAttribute(), tableName,
		 * dataPath); hclient.loadDataIntoTable(tableName, dataPath); } catch
		 * (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
	}

	private void registerView(String schemaStr, String tableName) {

		DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(
				DataSchema.class);
		DataSchema schema = parser.getDSConfigDetails(schemaStr);

		String[] args = new String[6];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home")
				+ "/zeas/Config/createHiveView.sh";
		args[2] = schema.getName() + "_view";
		args[3] = HiveClient.getColumnList(schema.getDataAttribute());
		args[4] = HiveClient.getCompositeKey(schema.getDataAttribute());
		args[5] = tableName;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		int status = shExe.runScript(args);
		if (status == 0) {
			logInfo.append(zdpLog.INFO("Register cleansed table :" + tableName));
		} else {
			logInfo.append(zdpLog.DEBUG("Not Register cleansed table :"
					+ tableName));
		}
		FileUtility.runLogAppend(logFilepath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
	}
	
	private void registerParquet(String columnNameAndDataType,String schemaStr, String tableName, String dataPath, String batchId) throws ZeasException {

		if (dataPath.charAt(dataPath.length() - 1) != '/') {
			dataPath = dataPath + "/";
		}

		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home")
				+ "/zeas/Config/createHiveParquetTable.sh";
		args[2] = tableName+"_parquet";
		args[3] = columnNameAndDataType;
		args[4] = tableName;
		args[5] = dataPath;
		args[6] = dataPath + batchId;
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
		
		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);		
		
	}
	
	private void registerQuarantine(String tableName, String dataPath,
			String batchId) {

		if (dataPath.charAt(dataPath.length() - 1) != '/') {
			dataPath = dataPath + "/";
		}
		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home")
				+ "/zeas/Config/createHiveTable.sh";
		args[2] = tableName;
		args[3] = HiveClient.getSchemaAttributes(schemaForQuarantine())
				.toString();
		args[4] = dataPath + "quarantine";
		args[5] = dataPath;
		args[6] = dataPath + batchId;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		int status = shExe.runScript(args);
		if (status == 0) {
			logInfo.append(zdpLog.INFO("Register quarantine table :"
					+ tableName));
		} else {
			logInfo.append(zdpLog.DEBUG("Not Register quarantine table :"
					+ tableName));
		}
		FileUtility.runLogAppend(logFilepath, "log.txt", logInfo.toString());
		logInfo = new StringBuilder();
		// HiveClient hclient = new HiveClient();
		// try {
		// hclient.createTable(schemaForQuarantine(), tableName, dataPath);
		// hclient.loadDataIntoTable(tableName, dataPath);
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	/** Simple driver to run as a stand alone example. */
	public static void main(String[] args) throws ZeasException {
		DataLoader loader = new DataLoader();
		Properties prop = new Properties();
		try {
			loader.readPropertiesFile(prop, PROP_FILE);
		} catch (IOException e) {
			LOG.error(e.toString());
		}

		loader.run(prop, args[0]);
	}

	private List<SchemaAttributes> schemaForQuarantine() {

		List<SchemaAttributes> attrs = new ArrayList<SchemaAttributes>();
		SchemaAttributes rule = new SchemaAttributes("Rule", "varchar");
		SchemaAttributes expected = new SchemaAttributes("Expected", "varchar");
		SchemaAttributes found = new SchemaAttributes("Found", "varchar");
		SchemaAttributes column = new SchemaAttributes("Column_Name", "varchar");
		SchemaAttributes time = new SchemaAttributes("errTime", "varchar");
		SchemaAttributes record = new SchemaAttributes("Record", "varchar");
		SchemaAttributes ingestionTime = new SchemaAttributes("IngestionTime",
				"Timestamp");
		SchemaAttributes fileName = new SchemaAttributes("sourceFile",
				"varchar");
		attrs.add(rule);
		attrs.add(expected);
		attrs.add(found);
		attrs.add(column);
		attrs.add(time);
		attrs.add(ingestionTime);
		attrs.add(fileName);
		attrs.add(record);
		return attrs;
	}
}
