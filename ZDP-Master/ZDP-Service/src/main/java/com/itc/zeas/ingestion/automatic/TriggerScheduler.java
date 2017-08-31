package com.itc.zeas.ingestion.automatic;

import com.itc.zeas.utility.EntityManagerHelper;
import com.itc.zeas.utility.CommonUtils;
import com.itc.zeas.utility.FileUtility;
import com.itc.zeas.utility.ZDPLog;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.rdbms.mysql.MysqlDataBaseUtility;
import com.itc.zeas.ingestion.automatic.rdbms.SqoopImportDetails;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.validation.rule.DataValidationConstant;
import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.taphius.databridge.model.DatasourceFileDetails;
import com.itc.zeas.utility.DBUtility;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.datachecker.FileSizeChecker;
import com.taphius.dataloader.DataLoader;
import com.taphius.dataloader.LoaderUtil;
import com.taphius.pipeline.HiveClient;
import com.taphius.validation.mr.IngestionLogDetails;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.utility.utility.ConfigurationReader;
import com.itc.zeas.utility.utility.UserProfileStatusCache;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @author Ketan on 4/29/2017.
 */
public class TriggerScheduler {
     Logger LOG = Logger.getLogger(TriggerScheduler.class);
    ZDPLog zdpLog = ZDPLog
            .getZDPLog("TriggerScheduler");
    public boolean runSchedular(String schedulerName, String userName, List<String> users, boolean isLocalUpload) throws Exception{

        String entityname="";
        if(isLocalUpload){
            entityname=schedulerName;
            schedulerName=entityname+"_Schedular";
        }
        else{
            entityname = schedulerName.substring(0,schedulerName.lastIndexOf("_"));
        }

        StringBuilder loginfo = new StringBuilder();
        LOG.info("Got trigger ingestion request for - " + schedulerName);
        // code added by deepak to check authorization for execute level
        // permission
        EntityManagerHelper entityManagerHelper = new EntityManagerHelper();

        loginfo.append(zdpLog.INFO("Ingestion requested by " + userName));
        loginfo.append(zdpLog.INFO("Ingestion in progress......"));
        Boolean haveValidPermission = entityManagerHelper
                .validateUserPermission(userName, "DataIngestion",
                        schedulerName, UserManagementConstant.READ_EXECUTE);
        // making entry into log logingestiondetails with log file,user status
        // etc.
        Long time = System.currentTimeMillis();
        String strTime = String.valueOf(time);
        ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();
        String logDirPath = ConfigurationReader.getProperty("RUN_LOG_PATH");
        LOG.debug("RUN_LOG_PATH: " + logDirPath);
        logDirPath = logDirPath + "/" + schedulerName + "/" + strTime;
        try {
            accessObjectImpl.createRunLogDetail(schedulerName, "ingestion",
                    "running", logDirPath, userName);
            loginfo.append(zdpLog.INFO("make entry RunLogDetail into database"));
            FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
            loginfo = new StringBuilder();
        } catch (Exception e1) {
            e1.printStackTrace();
            loginfo.append(zdpLog.ERROR("Error :" + e1.getMessage()));
            FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
            loginfo = new StringBuilder();
        }
        ZDPDataAccessObjectImpl accessObjectActivity=new ZDPDataAccessObjectImpl();
        DataLoader dataLoader = new DataLoader();
        CommonUtils utils = new CommonUtils();
        FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
        String datasetName=entityname+"_DataSet";
        String dataSetLocation="/user/zeas/"+userName+"/"+entityname;
        Timestamp timestamp=new Timestamp(new Date().getTime());
        IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
        DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);

        DataSchema schema = parser.getDSConfigDetails(DBUtility.getJSON_DATA(entityname));
        // Checking if ingestion type of local upload.
        if (isLocalUpload) {
            FileSizeChecker fileSizeChecker = new FileSizeChecker();
            boolean ingestionStatus= false;
            String fullPath = System.getProperty("user.home") + "/uploadData/" + entityname + "/";
            if(fullPath!=null){
                File file = new File(fullPath);
                ingestionStatus= fileSizeChecker.getFileSizeCount(file, entityname, userName);
            }
            if(ingestionStatus){
                ingestionLogDAO.addGraylogInfo(timestamp, userName, "ingestion", entityname, "Started");
                accessObjectImpl.addActivitiesBatchForNewAPI(entityname,
                        "Ingestion started for '" + entityname + "' by " + userName, ZDPDaoConstant.INGESTION_ACTIVITY,
                        ZDPDaoConstant.INITIATE_ACTIVITY, users, userName);
                accessObjectImpl.addComponentExecution(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, userName);
                // this the local intermediate path for file which is uploaded by
                // user by browser uploader.
                //String fullPath = System.getProperty("user.home") + "/uploadData/" + entityname + "/";
                Properties props = new Properties();
                props.setProperty(DataLoader.INGESTION_ID, DBUtility.getEntityId(schedulerName));
                props.setProperty(DataLoader.SOURCE_DIR, fullPath);
                props.setProperty(DataLoader.SCHEDULAR_NAME, schedulerName);
                props.setProperty(DataLoader.SCHEMA, entityname);
                props.setProperty(DataLoader.DATASET, datasetName);
                props.setProperty(DataLoader.HDFS_TARGET, dataSetLocation);
                props.setProperty(DataLoader.BATCH_FREQUENCY, "onetime");
                props.setProperty(DataLoader.FILE_FORMAT, schema.getFileData().getFileType());
                props.setProperty(DataLoader.FILE_TYPE, schema.getFileData().getFileType());
                props.setProperty(DataLoader.SCHEDULE_TIME, String.valueOf(System.currentTimeMillis()));
                props.setProperty(DataLoader.NAMENODE_HOST, ConfigurationReader.getProperty("HDFS_FQDN"));

                loginfo.append(zdpLog.INFO("Configuration Done"));
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
                // added by Deepak to handle First record Header
                // scenario
                final String IS_FIRST_RECORD_HEADER = schema.getFileData().gethFlag();
                props.setProperty(DataLoader.IS_FIRST_RECORD_HEADER, IS_FIRST_RECORD_HEADER);
                dataLoader.run(props, fullPath);
                // Removing key from chached map after completion of ingestion and
                // validation check.
                UserProfileStatusCache.removeKey(userName + "-" + entityname);
                return true;
            }
        }else{
            if (!haveValidPermission) {
                loginfo.append(zdpLog
                        .DEBUG("Ingestion fails due to permission issues"));
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
                accessObjectActivity.addComponentRunStatus(entityname,
                        ZDPDaoConstant.INGESTION_ACTIVITY,
                        ZDPDaoConstant.JOB_KILL_FAIL, "UNDEFINED", userName);
                String statusInfo = "Ingestion for '" + entityname + "' "
                        + "Fail";
                accessObjectActivity.addActivitiesBatchForNewAPI(
                        entityname, statusInfo,
                        ZDPDaoConstant.INGESTION_ACTIVITY,
                        ZDPDaoConstant.FAIL_ACTIVITY, users, userName);
                return haveValidPermission;
            }
            String[] strArry = null;
            SqoopImportDetails sqoop = new SqoopImportDetails();
            IngestionLogDetails logDetails = null;
            String batchId = "";
            loginfo = new StringBuilder();
            boolean isRdbms=false;
            String sourceType = utils.getSourceType(schedulerName);
            entityname = utils.getSchema();
            LOG.info("Source type: " + sourceType);
            loginfo.append(zdpLog.INFO("Source type: " + sourceType));
            switch (sourceType.toLowerCase()) {
                case "rdbms":
                    isRdbms=true;
                {
                    logDetails = new IngestionLogDetails();
                    loginfo.append(zdpLog.INFO("reading scoop import details"));

                    // call script and update record count to sent notifications.



                    strArry = sqoop.getDetailsForImport(schedulerName);
                    batchId = strArry[2];
                    loginfo.append(zdpLog.INFO("reading scoop import details"));
                    LOG.info("reading scoop import details");

                    /**
                     * Code for Range notification starts her
                     */
                    String rootPathScript = System.getProperty("user.home") + "/zeas/Config/" + entityname;
                    int recordCount  = getRecordCountForNotification(strArry,rootPathScript);

                    String notificationDetails[] = getNotificationDetails(accessObjectActivity,entityname);
                    if(notificationDetails !=null && notificationDetails.length>0){

                        int minSize = Integer.parseInt(notificationDetails[0]);
                        int maxSize = Integer.parseInt(notificationDetails[1]);
                        boolean isEmail = Boolean.parseBoolean(notificationDetails[2]);
                        boolean isAlert = Boolean.parseBoolean(notificationDetails[3]);
                        //boolean continueIngestion = Boolean.parseBoolean(notificationDetails[4]);

                        boolean isOutOfRange = getRangeFlag(recordCount, minSize, maxSize);


                        if(isOutOfRange){

                            if(isAlert){
                                String rangeMessage = getRangeMessage(entityname,recordCount,minSize,maxSize);
                                accessObjectActivity.addActivitiesBatchForNewAPI(entityname, rangeMessage, ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.INITIATE_ACTIVITY, users,userName);
                                //accessObjectActivity.addComponentExecution(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, userName);
                            }
                            if(isEmail){

                            }
                        }
				/*if(!continueIngestion){
					accessObjectActivity.addActivitiesBatchForNewAPI(entityname, "Ingestion can not be completed", ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.INITIATE_ACTIVITY, users,userName);
					return false;
				}*/
                    }

                    /**
                     * Ends here
                     */

                    if (strArry != null) {
                        loginfo.append(zdpLog.INFO("Import information"));
                        for (int i = 0; i < strArry.length; i++) {
                            loginfo.append("strArr[" + i + "] :" + strArry[i]+"\n");
                        }
                    }
                    logDetails.setIngestionStart("Ingestion started on |"
                            + new Timestamp(System.currentTimeMillis()));
                    loginfo.append(zdpLog.INFO("Ingestion started on |"
                            + new Timestamp(System.currentTimeMillis())));
                    ingestionLogDAO.addLogObject(utils.getSchedulerId(), batchId,
                            "Ingestion", "Started", logDetails);
                    timestamp=new Timestamp(new Date().getTime());
                    ingestionLogDAO.addGraylogInfo(timestamp, userName, "ingestion", entityname, "Started");
                    LOG.info("User " + userName + ": Ingestion started for '"
                            + entityname + "'. IngestionType: rdbms");
                    loginfo.append(zdpLog.INFO("User " + userName
                            + ": Ingestion started for '" + entityname
                            + "'. IngestionType: rdbms"));
                    if(users !=null && users.size()>0){
                        accessObjectActivity.addActivitiesBatchForNewAPI(entityname, "Ingestion started for '"+ entityname+"' by "+userName, ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.INITIATE_ACTIVITY, users,userName);
                        accessObjectActivity.addComponentExecution(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, userName);
                    }
                    FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                    loginfo = new StringBuilder();
                }
                break;
                case "file": {
                    FileSizeChecker fileSizeChecker = new FileSizeChecker();
                    ZDPDataAccessObjectImpl zdp = new ZDPDataAccessObjectImpl();
                    String jsonStr = zdp.getJSONFromEntity(entityname + "_Source");
                    String location = null;
                    boolean ingestionStatus = false;
                    if (jsonStr != null) {
                        JSONObject jsonObj;
                        try {
                            jsonObj = new JSONObject(jsonStr);
                            location = jsonObj.getString("location");
                        } catch (JSONException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                    }
                    if(location!=null){
                        File file = new File(location);
                        ingestionStatus= fileSizeChecker.getFileSizeCount(file, entityname , userName);
                    }
                    if(ingestionStatus){
                        LOG.info("Creating _DONE file");
                        loginfo.append(zdpLog.INFO(" _DONE file creation in progress."));
                        strArry = new String[3];
                        String SQOOP_SCRIPT_PATH = System.getProperty("user.home") + "/zeas/Config/filecreate.sh";
                        String SHELL_SCRIPT_TYPE = "/bin/bash";
                        strArry[0] = SHELL_SCRIPT_TYPE;
                        strArry[1] = SQOOP_SCRIPT_PATH;

                        // check ingestion type from Local upload or NFS ingestion.
                        if (isLocalUpload) {
                            strArry[2] = System.getProperty("user.home") + "/data/" + utils.getSchema() + "/";
                        } else {
                            strArry[2] = utils.getSourceFile();
                        }

                        ingestionLogDAO = new IngestionLogDAO();
                        timestamp = new Timestamp(new Date().getTime());
                        ingestionLogDAO.addGraylogInfo(timestamp, userName, "ingestion", entityname, "Started");
                        accessObjectImpl.addComponentExecution(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, userName);
                        loginfo.append(zdpLog.INFO(" ingestion," + entityname + " Started on " + timestamp));
                        LOG.info("User " + userName + ": Ingestion started for '" + entityname + "'. IngestionType: file");
                        loginfo.append(zdpLog.INFO(
                                "User " + userName + ": Ingestion started for '" + entityname + "'. IngestionType: file"));
                        if (users != null && users.size() > 0) {
                            accessObjectActivity.addActivitiesBatchForNewAPI(entityname,
                                    "Ingestion started for '" + entityname + "' by " + userName,
                                    ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.INITIATE_ACTIVITY, users, userName);
                        }
                    }
                }
                break;
                default:
                    LOG.error("Invalid source type.");
                    loginfo.append(zdpLog.DEBUG("Invalid source type."));
            }
            BufferedReader brError = null;
            BufferedReader br = null;
            try {

			/*
			 * Pass string array as parameter to the script. First argument
			 * should be type of script, second should be path of script and
			 * rest arguments should be user defined.
			 */
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
                loginfo.append(zdpLog.DEBUG("initializing the script."));
                if(isRdbms){
                    loginfo.append(zdpLog.DEBUG("sqoop script started."));
                }
                // Running script using process builder
                ProcessBuilder pb = new ProcessBuilder(strArry);
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                pb.redirectErrorStream(true);
                Process p = null;
                String text="";
                String jobId="";
                try {
                    p = pb.start();
                    br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    while((text=br.readLine())!=null){
                        if(text.contains("Running job:")){
                            int start=text.lastIndexOf(":")+1;
                            jobId=text.substring(start, text.length()).trim();
                            break;
                        }
                    }
                } catch (IOException e) {
                }
                if(isRdbms){
                    accessObjectActivity.addComponentRunStatus(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.DATA_IMPORTING, jobId, userName);
                }
                loginfo = new StringBuilder();
                int status = p.waitFor();
                // Print out put of script execution on screen.
                while (br.ready()) {
                    String strLog = br.readLine();
                    LOG.info(strLog);
                    loginfo.append(strLog + "\n");
                }
                // Logging the errors, if any while script execution.
                brError = new BufferedReader(new InputStreamReader(
                        p.getErrorStream()));
                while (brError.ready()) {
                    String strErr = brError.readLine();
                    LOG.info(strErr);
                    loginfo.append(strErr + "\n");
                }
                brError.close();
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
                if (!isRdbms) {
                    if (status == 0) {
                        loginfo.append(zdpLog.INFO(" _DONE file created successfully"));

                    } else {
                        loginfo.append(zdpLog.DEBUG(" _DONE file created failed"));
                    }
                } else {									 // If Rdbms type, run validation rules.
                    loginfo.append(zdpLog.DEBUG("sqoop import script finished. \n"));
                    if (status == 0) {
                        logDetails.setIngestionComplete("Ingestion complete |"
                                + new Timestamp(System.currentTimeMillis()));
                        ingestionLogDAO.updateLogObject(utils.getSchedulerId(),
                                batchId, "Ingestion", "Completed", logDetails);
                        loginfo.append(zdpLog.INFO(" Ingestion Completed on "+ new Timestamp(System.currentTimeMillis())+"\n"));
                        LOG.info("User " + userName + ": Ingestion completed for '"+ entityname + "'. IngestionType: rdbms");
                        FileUtility.runLogAppend(logDirPath, "log.txt",	loginfo.toString());
                        loginfo = new StringBuilder();
                        String targetPath = strArry[3];
                        String yarnClassPaths="";
                        String[] args1 = new String[2];
                        args1[0] = ShellScriptExecutor.BASH;
                        args1[1] = System.getProperty("user.home")+"/zeas/Config/getYarnClassPath.sh";

                        pb = new ProcessBuilder(args1);
                        pb.redirectErrorStream(true);
                        p = null;
                        try {
                            p = pb.start();
                            br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                            text=br.readLine();
                            if(text!=null){
                                yarnClassPaths=text;
                            }
                        } catch (IOException e) {
                        }
                        String[] args = new String[9];
                        args[0] = targetPath;
                        args[1] = utils.getSchema();
                        args[2] = "onetime";
                        args[3] = DataValidationConstant.RDBMS;
                        args[4] = LoaderUtil.getIngestionTime().toString();
                        args[5] = "";
                        args[6] = batchId;
                        args[7]=yarnClassPaths;
                        //this flag indicates if first row is Header.
                        //but for RDBMS source this doesn't make sense.
                        args[8] = "false";

                        String dataSet = utils.getDataSet();
                        String ingestionId = Integer.toString(utils
                                .getSchedulerId());

                        // Pass required information to run validation rules
                        String columnNameAndDataType = HiveClient
                                .getSchemaAttributes(schema.getDataAttribute())
                                .toString()
                                + ",ingestionTime timestamp" + ",sourceFile string";
                        if (schema.getQuery() != null
                                || "".equalsIgnoreCase(schema.getQuery())) {
                            columnNameAndDataType = getcolumnNameAndDataType(schema);
                        }
                        loginfo.append(zdpLog
                                .INFO("calling runValidationRules api for validation of data"));
                        FileUtility.runLogAppend(logDirPath, "log.txt",
                                loginfo.toString());
                        loginfo = new StringBuilder();
                        String profileName=args[1];
                        dataLoader.runValidationRules(columnNameAndDataType,args, dataSet, ingestionId,
                                batchId, logDetails, ingestionLogDAO,profileName,
                                users,schedulerName,logDirPath,userName);
                    } else {
                        logDetails.setIngestionComplete("Ingestion failed |"
                                + new Timestamp(System.currentTimeMillis()));
                        ingestionLogDAO.updateLogObject(utils.getSchedulerId(),
                                batchId, "Ingestion", "Failed", logDetails);
                        loginfo.append(zdpLog.INFO("Ingestion Failed on "+ new Timestamp(System.currentTimeMillis())+"\n"));
                        accessObjectActivity.addActivitiesBatchForNewAPI(entityname, "Ingestion for "+entityname+" failed", ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.FAIL_ACTIVITY,users, userName);
                        accessObjectActivity.addComponentRunStatus(entityname, ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.JOB_KILL_FAIL, jobId, userName);
                        LOG.info("User " + userName + ": Ingestion failed for '"
                                + entityname + "'. IngestionType: rdbms");
                    }
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
                loginfo.append(e.toString() + "\n");
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();

            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
                loginfo.append(e.toString() + "\n");
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
            } catch (SQLException e) {
                e.printStackTrace();
                loginfo.append(e.toString());
                FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
                loginfo = new StringBuilder();
            }
            finally{
                //closing the bufferredreader connections
                if(br != null){
                    br.close();
                }
                if(brError != null){
                    brError.close();
                }
            }
        }

        FileUtility.runLogAppend(logDirPath, "log.txt", loginfo.toString());
        loginfo = new StringBuilder();
        return true;
    }
    private boolean getRangeFlag(int recordCount, int min, int max) {

        if(recordCount <min || recordCount >max)
            return true;

        return false;
    }
    public String getRangeMessage(String entityname, int recordCount, int min, int max) {

        StringBuilder messageStr = new StringBuilder();
        if (recordCount<min){
            messageStr.append("record in table ");
            messageStr.append(recordCount);
            messageStr.append(" for ");
            messageStr.append(entityname);
            messageStr.append(" is less than minimum record range ");
            messageStr.append(min);

            return messageStr.toString();
        }

        else{
            messageStr.append("record in table ");
            messageStr.append(recordCount);
            messageStr.append(" for ");
            messageStr.append(entityname);
            messageStr.append(" is more than maximum record range ");
            messageStr.append(max);
            return messageStr.toString();
        }

    }

    public int getRecordCountForNotification(String[] arry, String path) {

        String localArr[] =  arry.clone();;

        int recordCount =0;

        BufferedReader brError = null;
        BufferedReader br = null;

		/*
		 * Pass string array as parameter to the script. First argument should
		 * be type of script, second should be path of script and rest arguments
		 * should be user defined.
		 */
        // Running script using process builder
        localArr[1]=path+"/SqoopEval.sh";

        ProcessBuilder pb = new ProcessBuilder(localArr);
        pb.redirectErrorStream(true);
        Process p = null;
        String text = "";
        String jobId = "";

        ArrayList<String> lineList = new ArrayList<String>();
        try {
            p = pb.start();
            br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((text = br.readLine()) != null ) {
                System.out.println(text);

                //recordCount = Integer.parseInt(text);
                text =text.replaceAll("\\s+","").trim();
                lineList.add(text);

            }
            String recordCount1 = lineList.get(lineList.size()-2);
            //value will be line '|36|'
            recordCount  = Integer.parseInt(recordCount1.substring(1,recordCount1.lastIndexOf("|")));


        } catch (IOException e) {
        }
        try {
            int status = p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Print out put of script execution on screen.
        try {
            while (br.ready()) {
                String strLog = br.readLine();
                LOG.info(strLog);
            }
            // Logging the errors, if any while script execution.
            brError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            while (brError.ready()) {
                String strErr = brError.readLine();
                LOG.info(strErr);
            }

            brError.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException logOrIgnore) {
                }
        }
        return recordCount;

    }
    public String [] getNotificationDetails(ZDPDataAccessObjectImpl accessObjectActivity, String entityname) {
        String details [] = new String[5];

        try {
            JSONObject jsonObject = new JSONObject(accessObjectActivity.getJSONFromEntity(entityname+"_Source"));
            if(jsonObject !=null){
                JSONObject fileData = jsonObject.getJSONObject("fileData");

                boolean notificationFlag = Boolean.parseBoolean(fileData.getString("notificationSet"));

                if(!notificationFlag)
                    return null;

                String minSize = fileData.getString("minsize");
                String maxSize = fileData.getString("maxsize");
                String emailNotify = fileData.getString("notifyEmail");
                String alertNotify = fileData.getString("notifyAlert");
                //String contIngestion = fileData.getString("contIngestion");

                if(minSize !=null)
                    details[0] = minSize;
                if(maxSize !=null)
                    details[1] = maxSize;
                if(emailNotify !=null)
                    details[2] = emailNotify;
                if(alertNotify !=null)
                    details[3] = alertNotify;
				/*if(contIngestion !=null)
					details[4] = contIngestion;*/
            }
            return details;

        } catch (JSONException e) {
            return null;
        }

    }
    public String getcolumnNameAndDataType(DataSchema schema) {

        StringBuilder builder = new StringBuilder();
        if (schema.getFileData() != null) {
            DatasourceFileDetails fileDetails = schema.getFileData();
            String hostUrl = fileDetails.getHostName();
            String databaseName = fileDetails.getDbName();
            String userName = fileDetails.getUserName();
            String password = fileDetails.getPassword();
            String dbPort = fileDetails.getPort();
            Connection conn = null;
            Statement st = null;
            ResultSet rs = null;



            String dbType = fileDetails.getDbType();
            try{
                switch (dbType) {
                    case FileReaderConstant.MYSQL_TYPE: {
                        Class.forName(FileReaderConstant.MYSQL_DRIVER);
                        conn = DriverManager.getConnection("jdbc:mysql://" + hostUrl + ":" + dbPort, userName, password);
                    }
                    break;
                    case FileReaderConstant.ORACLE_TYPE: {
                        Class.forName(FileReaderConstant.ORACLE_DRIVER);
                        conn = DriverManager.getConnection("jdbc:oracle:thin:@" + hostUrl + ":" + databaseName, userName,
                                password);

                    }
                    break;
                    case FileReaderConstant.DB2_TYPE: {
                        Class.forName(FileReaderConstant.DB2_DRIVER);
                        conn = DriverManager.getConnection("jdbc:db2://" + hostUrl + ":" + dbPort, userName, password);
                    }
                    break;
                }



                st = conn.createStatement();
                st.execute("use " + databaseName);
                rs = st.executeQuery(schema.getQuery() + " where 1=2");
                ResultSetMetaData rsMetaData = rs.getMetaData();

                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    String name = rsMetaData.getColumnName(i);
                    String dataType = "";
                    String actulaDataType = rsMetaData.getColumnTypeName(i);
                    try {
                        dataType = MysqlDataBaseUtility.MysqlDataTypeMap.valueOf(actulaDataType)
                                .getValue();
                    } catch (IllegalArgumentException e) {
                        String[] dt = actulaDataType.split(" ");
                        dataType = MysqlDataBaseUtility.MysqlDataTypeMap.valueOf(dt[0]).getValue();
                        e.printStackTrace();
                    }
                    if (name.contains("(") || name.contains(")")) {
                        name = name.replace("(", "_of_");
                        name = name.replace(")", "");
                    }
                    builder.append(name + " " + dataType + ",");
                }

            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return builder.toString() + "ingestionTime timestamp"
                + ",sourceFile string";
    }
}
