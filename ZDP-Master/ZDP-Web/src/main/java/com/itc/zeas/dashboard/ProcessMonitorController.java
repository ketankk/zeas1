package com.itc.zeas.dashboard;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.exceptions.ZeasErrorCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.dashboard.dashboard.ProcessMonitorService;
import com.itc.zeas.exceptions.SqlIoException;

@RestController
@RequestMapping("/rest/service/dashboard")
public class ProcessMonitorController {

		/*
		 * send the response to UI about the number of running process/job. 
		 */

    @RequestMapping(value = "/getNoOfRunningProcesses/", method = RequestMethod.GET, headers = "Accept=application/json")
    public ResponseEntity<?> getNoOfRunningProcesses(HttpServletRequest httpServletRequest) throws IOException {

        ProcessMonitorService monitorService = new ProcessMonitorService();
        CommonUtils commonUtils = new CommonUtils();
        String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
        try {
            return ResponseEntity.ok(monitorService.getNoOfRunningProcesses(userName));
        } catch (Exception e) {
            return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

    }
       
	/*
	 * send the response to UI about the running job details based on search
	 * type . search type can be any of these : streaming, ingestion,project
	 */

    @RequestMapping(value = "/getRunningProcesses/{searchType}", method = RequestMethod.GET, headers = "Accept=application/json")
    public ResponseEntity<Object> getRunningProcesses(@PathVariable("searchType") String searchType, HttpServletRequest httpServletRequest) throws IOException {

        ProcessMonitorService monitorService = new ProcessMonitorService();
        CommonUtils commonUtils = new CommonUtils();
        String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
        try {
            return monitorService.getRunningProcesses(userName, "", searchType.toLowerCase(), false);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
       
       /*
   	 * This service is to stop any running process.
   	 * parameters to service is search type(e.g ingestion,project,streaming), 
   	 * jobId is application id (hadoop application/job id)
   	 */

    @RequestMapping(value = "/stopRunningProcesses/{searchType}/{jobId}", method = RequestMethod.GET, headers = "Accept=application/json")
    public boolean stopRunningProcesses(@PathVariable("searchType") String searchType,
                                        @PathVariable("jobId") String jobId, HttpServletRequest httpServletRequest) throws IOException, InterruptedException, SQLException {
        ProcessMonitorService monitorService = new ProcessMonitorService();
        CommonUtils commonUtils = new CommonUtils();
        String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
        return monitorService.killRunningJob(searchType.toLowerCase(), jobId, userName);
    }

    /*
        * This service is to stop any running process.
        * parameters to service is search type(e.g ingestion,project,streaming),
        * entityId is particular entity id (ingestion profile id,project profile id)
        */
    @RequestMapping(value = "/stopProcess/{searchType}/{entityId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    public ResponseEntity<String> stopProcess(@PathVariable("searchType") String searchType,
                                              @PathVariable("entityId") Long entityId, HttpServletRequest httpServletRequest)
            throws Exception {

        ProcessMonitorService monitorService = new ProcessMonitorService();
        ZDPUserAccessImpl zdpUserAccessImpl = new ZDPUserAccessImpl();
        CommonUtils commonUtils = new CommonUtils();
        Boolean haveValidPermission = false;
        String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);

        try {
            haveValidPermission = zdpUserAccessImpl.validateUserPermissionForResource(
                    searchType.equalsIgnoreCase("ingestion") ? UserManagementConstant.ResourceType.DATASET
                            : UserManagementConstant.ResourceType.PROJECT,
                    userName, entityId, UserManagementConstant.READ_EXECUTE);
        } catch (SqlIoException.IoException exception) {
            new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (SqlIoException.SqlException exception) {
            new ResponseEntity<Object>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        ResponseEntity<String> responseEntity = null;
        String jobId = "";
        if (haveValidPermission) {
            jobId = monitorService.getJobId(entityId, searchType.toLowerCase());
            boolean status = jobId.isEmpty() ? false : monitorService.killRunningJob(searchType.toLowerCase(), jobId, userName);
            // status=true;
            if (status) {
                responseEntity = new ResponseEntity<String>("Process terminated.", HttpStatus.OK);
            } else {
                responseEntity = new ResponseEntity<String>("Termination of running process not possible.",
                        HttpStatus.OK);
            }
        } else {
            // don't have valid permission to execute the project
            responseEntity = new ResponseEntity<String>("don't have enough permission to stop the process",
                    HttpStatus.FORBIDDEN);
        }
        return responseEntity;
    }


}

