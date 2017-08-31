package com.itc.zeas.ingestion;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.TriggerScheduler;
import com.itc.zeas.ingestion.model.ArchivedFileInfo;
import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.file.FileCopyFromLocal;
import com.itc.zeas.profile.model.SampleData;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.utils.CommonUtils;
import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.validation.mr.IngestionLogDetails;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ketan on 4/30/2017.
 */
@RestController
@RequestMapping("/rest/service")
public class IngestionController {

    private static Logger LOG=Logger.getLogger(IngestionController.class);
    private TriggerScheduler triggerScheduler;

    /**
     * Data ingestion details this method list out log details for any data
     * ingestion
     *
     * @param entityId
     * @return List<DataIngestionLog>
     */

    @RequestMapping(value = "/listIngestionDetails/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
    public @ResponseBody
    IngestionLogDetails listIngestionDetails(@PathVariable("entityId") Integer entityId) {

        IngestionLogDAO logDAO = new IngestionLogDAO();
        return logDAO.getLogObject(entityId);
    }

    /**
     * This method takes care of delete Schema functionality. Its handled like
     * this - a)move HDFS dataset from Target HDFS path to pre-defined local
     * archive dir b)move definitions from Entity table to other table
     * schema_archive
     *
     * @param entity
     * @return
     */
    @RequestMapping(value = "/moveToArchive", method = RequestMethod.POST, headers = "Accept=application/json")
    public @ResponseBody void moveToArchive(@RequestBody Entity entity, HttpServletRequest httpServletRequest,
                                            HttpServletResponse response) {
        // added by to deepak to verify creator of dataset start
        Boolean canDelete = false;
        String entityName = entity.getName();
        CommonUtils commonUtils = new CommonUtils();
        String user = commonUtils.extractUserNameFromRequest(httpServletRequest);

        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        try {
            canDelete = zdpUserAccess.canDeleteDatset(entityName, user);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        PrintWriter printObj = null;
        if (canDelete) {
            // added by deepak ends
            String destDir = entity.getLocation();
            String userName = entity.getCreatedBy();
            String schemaName = entity.getName();
            String ids = entity.getJsonblob();
            String[] str = ids.split(",");
            String dataSetId = str[0];
            String dataSourceId = str[1];
            String dataSchemaId = str[2];
            String dataSchedularId = str[3];
            try {
                printObj = response.getWriter();
                EntityManager entitymanager = new EntityManager();
                entitymanager.moveToArchive(userName, dataSetId, dataSourceId, dataSchemaId, dataSchedularId,
                        schemaName, destDir);
            } catch (Exception e) {
                String errorMessage = "An error occurred at the server.";
                printObj.println(errorMessage);
                if (e instanceof ZeasException) {
                    if (((ZeasException) e).getErrorCode() == ZeasErrorCode.SQL_EXCEPTION) {
                        errorMessage = e.getMessage();
                        printObj.println(errorMessage);
                    } else if (((ZeasException) e).getErrorCode() == ZeasErrorCode.ZEAS_EXCEPTION) {
                        errorMessage = e.getMessage();
                        printObj.println(errorMessage);
                    }
                } else {
                    errorMessage = "Error in performing delete operation.";
                    printObj.println(errorMessage);
                }
                LOG.error("Move to archive failed." + e.toString());
                response.setStatus(ZeasErrorCode.FILE_NOT_FOUND);
                printObj.println(errorMessage);
            }
        } else {
            try {
                printObj = response.getWriter();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String errorMessage = "Failed! You need to be creator of this ingestion.";
            response.setStatus(403);
            printObj.println(errorMessage);
        }
    }

    @RequestMapping(value = "/moveDataToArchive", method = RequestMethod.POST, headers = "Accept=application/json")
    public @ResponseBody void moveDataToArchive(@RequestBody Entity entity, HttpServletRequest httpServletRequest,
                                                HttpServletResponse response) {
        // added by to deepak to verify creator of dataset start
        String destDir = entity.getLocation();
        String ids = entity.getJsonblob();
        String[] str = ids.split(",");
        String dataSchemaId = str[2];
        try {
            EntityManager entitymanager = new EntityManager();
            entitymanager.moveDataToArchive(destDir, dataSchemaId);
        } catch (Exception e) {
            String errorMessage = "An error occurred at the server.";
            if (e instanceof ZeasException) {
                if (((ZeasException) e).getErrorCode() == ZeasErrorCode.SQL_EXCEPTION) {
                    errorMessage = e.getMessage();
                } else if (((ZeasException) e).getErrorCode() == ZeasErrorCode.ZEAS_EXCEPTION) {
                    errorMessage = e.getMessage();
                }
            } else {
                errorMessage = "Error in performing delete operation.";
            }
            LOG.error("Move to archive failed." + e.toString());
            response.setStatus(ZeasErrorCode.FILE_NOT_FOUND);
            try {
                response.getWriter().print(errorMessage);
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (Exception exception) {
                exception.printStackTrace();
            }

        }

    }

    /**
     * This service will provide list of archive profiles
     *
     * @return map of schema id and name
     * @throws ZeasException
     * @throws SQLException
     * @throws IOException
     */
    @RequestMapping(value = "/ListArchiveProfiles", method = RequestMethod.GET, headers = "Accept=application/json")
    public List<ArchivedFileInfo> getArchiveProfiles(HttpServletRequest httpServletRequest,
                                                     HttpServletResponse response) throws ZeasException, IOException, SQLException {

        LOG.debug("inside function getArchiveProfiles");
        List<ArchivedFileInfo> archivedFileInfos = null;
        try {
            EntityManager entityManager = new EntityManager();
            archivedFileInfos = entityManager.getArchiveProfiles(httpServletRequest);

        } catch (ZeasSQLException e) {
            response.setStatus(e.getErrorCode());
            try {
                response.getWriter().print(e.toString());
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return archivedFileInfos;
    }

    /**
     * This service will restore the archive data - data is moved from archived
     * location to hdfs location It also takes care of inserting schema, source,
     * dataset and scheduler json information to database.
     * <p>
     * id - schema that should be restored
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/restoreArchive/{schemaDataId}", method = RequestMethod.POST, headers = "Accept=application/json")
    public String restoreArchive(@PathVariable String schemaDataId, HttpServletRequest request,
                                 HttpServletResponse response) throws IOException {

        int schemaId = Integer.parseInt(schemaDataId);
        // PrintWriter printObj = null;
        LOG.debug("restoreArchivedData: schemaid: " + schemaId);
        String hdfsPath = "";
        try {
            // printObj = response.getWriter();
            EntityManager entityManager = new EntityManager();
            hdfsPath = entityManager.getHdfsPath(schemaId);

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
            FileSystem hdfs = FileSystem.get(conf);
            Path hdfsDirPath = new Path(hdfsPath);

            if (!(hdfs.exists(hdfsDirPath))) {
                entityManager.restoreArchive(schemaId, request);
            } else {
                LOG.info("EntityController: restoreArchivedData: hdfs path is already in use: ");
                response.setStatus(ZeasErrorCode.ZEAS_EXCEPTION);
                response.getWriter().print("-Path: " + "\"" + hdfsPath + "\""
                        + " is already in use, please clean up data to import schema");
            }

        } catch (ZeasSQLException e) {
            e.printStackTrace();
            response.setStatus(e.getErrorCode());
            response.getWriter().print(e.toString());
            LOG.info("EntityController.restoreArchivedData(): ZeasSQLException: " + e.getMessage());
        } catch (ZeasException e) {
            e.printStackTrace();
            response.setStatus(e.getErrorCode());
            response.getWriter().print(e.toString());
            LOG.info("EntityController.restoreArchivedData(): Exception: " + e.getMessage());
        } catch (IOException | SQLException e) {
            LOG.info("EntityController.restoreArchivedData(): Throwable: " + e.getMessage());
            response.setStatus(ZeasErrorCode.ZEAS_EXCEPTION);
            response.getWriter().print("Processing Request Failed. Please check hadoop Configuration. Refer LOGS");
        }

        return hdfsPath;
    }

    @RequestMapping(value = "/restoreArchivedData", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public void restoreArchivedData(@RequestBody Entity entity, HttpServletRequest request,
                                    HttpServletResponse response) throws IOException {

        String schemaId = entity.getName();
        String hdfsPath = entity.getLocation();
        LOG.debug("restoreArchivedData: schemaid: " + schemaId);
        System.out.println("schemaId************************************hdfsPath" + schemaId
                + "************************************" + hdfsPath);

        try {
            EntityManager entityManager = new EntityManager();
            // String hdfsPath = entityManager.getHdfsPath(schemaId);

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
            FileSystem hdfs = FileSystem.get(conf);
            Path hdfsDirPath = new Path(hdfsPath);

            if (!(hdfs.exists(hdfsDirPath))) {
                entityManager.restoreDataFromArchive(schemaId, hdfsPath);
            } else {
                LOG.info("EntityController: restoreArchivedData: hdfs path is already in use: ");
                response.setStatus(ZeasErrorCode.ZEAS_EXCEPTION);
                response.getWriter().println("hdfs path is already in use");
            }
        } catch (ZeasSQLException e) {
            e.printStackTrace();
            response.setStatus(e.getErrorCode());
            LOG.info("EntityController.restoreArchivedData(): ZeasSQLException: " + e.getMessage());
            response.getWriter().println(e.getMessage());
        } catch (ZeasException e) {
            e.printStackTrace();
            response.setStatus(e.getErrorCode());
            LOG.info("EntityController.restoreArchivedData(): Exception: " + e.getMessage());
            response.getWriter().println(e.getMessage());
        }
    }
    /**
     * This method is used to trigger the Ingestion Process. Internally it calls
     * a script which touches _DONE file at the Source dir.
     *
     *            name of Scheduler.
     */
    // @RequestMapping(value="/testRun", method =
    // RequestMethod.POST,headers="Accept=application/json")
    @RequestMapping(value = "/testRunIngestion", method = RequestMethod.POST, headers = "Accept=application/json")
    public @ResponseBody
    ResponseEntity<?> triggerTestRun(@RequestBody SampleData fileObject) {

        try {
			LOG.info("request object for test run:" + fileObject);
			SampleData sample = new SampleData();

			if (fileObject.getFileName() != null) {
			    System.out.println("IngestionTriggerController.triggerTestRun(): "
			            + fileObject.getFileName());
			} else {
			    System.out
			            .println("IngestionTriggerController.triggerTestRun(): null");
			}
			if (fileObject.getFileName() != null) {
			    // FileCopyFromLocal.method(file.getFileName());
			    String[] strArr = fileObject.getFileName().split("/");
			    String fileName = strArr[strArr.length - 1];
			    FileCopyFromLocal copyFromLocal = new FileCopyFromLocal();
			    if (fileName.endsWith("csv")) {
			        fileName = fileName.replace(".csv",
			                FileReaderConstant.SAMPLE_FILE);
			    } else if (fileName.endsWith("xls")) {
			        fileName = fileName.replace(".xls", "_" + 1
			                + FileReaderConstant.SAMPLE_FILE);
			    } else if (fileName.endsWith("xlsx")) {
			        fileName = fileName.replace(".xlsx", "_" + 1
			                + FileReaderConstant.SAMPLE_FILE);
			    } else if (fileName.endsWith("json")) {
			        fileName = fileName.replace(".json",
			                FileReaderConstant.SAMPLE_FILE);
			    } else if (fileName.endsWith("xml")) {
			        fileName = fileName.replace(".xml",
			                FileReaderConstant.SAMPLE_FILE);
			    } else if (fileName.endsWith("database")) {
			        fileName = fileName.replace(".database",
			                FileReaderConstant.SAMPLE_FILE);
			    }
			    LOG.info("filename:" + fileObject.getFileName());
			    sample = copyFromLocal.copyFromLocal(
			            ConfigurationReader.getProperty("APP_DIR") + File.separator
			                    + fileName, fileObject.getTargetPath()
			                    + File.separator + "TestRun" + File.separator
			                    + fileName);
			    sample.setFileName(fileName);
			}
			return ResponseEntity.ok(sample);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
    }
    /**
     * This method is used to trigger the Ingestion Process. Internally it calls
     * a script which touches _DONE file at the Source dir.
     *
     *            name of Scheduler.
     * @throws SQLException
     * @throws IOException
     */
    @RequestMapping(value = "/runScheduler/{schedulerName}", method = RequestMethod.POST, headers = "Accept=application/json")
    public @ResponseBody
    ResponseEntity<?> triggerScheduler(@PathVariable("schedulerName") String schedulerName,
                             HttpServletRequest httpServletRequest) throws IOException,
            SQLException {
        com.itc.zeas.utility.CommonUtils commonUtils = new com.itc.zeas.utility.CommonUtils();
        String userName = commonUtils
                .extractUserNameFromRequest(httpServletRequest);
        List<String> users=new ArrayList<>();
        users.add(userName);
        boolean flag =false;
        try {
        	flag = new TriggerScheduler().runSchedular(schedulerName,userName,users,false);
            return ResponseEntity.ok(flag);
        } catch (ZeasException e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
        return ResponseEntity.ok(flag);
    }

}
