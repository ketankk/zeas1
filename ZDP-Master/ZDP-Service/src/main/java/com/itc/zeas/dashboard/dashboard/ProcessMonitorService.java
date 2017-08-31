package com.itc.zeas.dashboard.dashboard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.streaming.model.StreamingEntity;
import com.itc.zeas.utility.utility.ConfigurationReader;
import com.itc.zeas.dashboard.model.RunningProcess;
public class ProcessMonitorService {
	/*
	 * This method will return List of running process.
	 * Map<String, Long> will contain key as the type of process (eg. ingestion,project,streaming)
	 * value is count of running process.
	 */
	public Map<String, Long> getNoOfRunningProcesses(String userName) throws Exception {
		Map<String, Long> runningProcesses=new HashMap<>();
		Connection connection = null;
		PreparedStatement preparedStatement=null;
		ResultSet resultSet=null;
		
		try {
			String query=ConnectionUtility.getSQlProperty("GET_TOTAL_RUNNING_PROCESSES");
			System.out.println(query);
			connection=ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, userName);
			resultSet = preparedStatement.executeQuery();
			while(resultSet.next()){
				String compType=resultSet.getString("comp_type");
				long runCount=resultSet.getLong("run_count");
				runningProcesses.put(compType, runCount);
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}finally{
			ConnectionUtility.releaseConnectionResources(resultSet,preparedStatement, connection);
		}
		return runningProcesses;
	}
	
	/*
	 * this method returns details about process running.
	 * return is list of object of RunningProcess type or StreamDriver type
	 */
	public ResponseEntity<Object> getRunningProcesses(String userName,String consumerName,String type,boolean isStreaming) throws Exception {
		List<RunningProcess> runningProcessList=new ArrayList<>();
		List<StreamingEntity> drivers=new ArrayList<>();
		ResponseEntity<Object> responseEntity=null;
		Connection connection = null;
		PreparedStatement preparedStatement=null;
		ResultSet resultSet=null;
		String tablename="entity";
		switch (type) {
		case ZDPDaoConstant.ZDP_INGESTION:
			tablename = "entity";
			break;
		case ZDPDaoConstant.STREAM_ACTIVITY:
			tablename = "entity";
			break;
		case ZDPDaoConstant.ZDP_PROJECT:
			tablename = "project";
			break;
		}
		try {
			String query=ConnectionUtility.getSQlProperty("GET_PROCESS_DETAILS_BY_TYPE");
			if(query!=null && query.contains("tablename")){
				query=query.replace("tablename", tablename);
			}
			connection=ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, userName);
			preparedStatement.setString(2, type.toUpperCase());
			resultSet = preparedStatement.executeQuery();
			while(resultSet.next()){
				String compName=resultSet.getString("comp_name");
				String status=resultSet.getString("status");
				 Timestamp runTime = resultSet.getTimestamp("time_stamp");
				 String jobId=resultSet.getString("job_id");
				 if(isStreaming){
					 StreamingEntity driver = new StreamingEntity();
						driver.setId(Integer.parseInt(jobId));
						driver.setName(compName);
						driver.setStartAt(runTime.toString());
						driver.setStartBy(userName);
						drivers.add(driver);
				 }else{
					 RunningProcess process=new RunningProcess();
					 process.setName(compName);
					 process.setStartedBy(userName);
					 process.setStartedOn(runTime);
					 process.setStatus(status);
					 process.setJobId(jobId);
					 runningProcessList.add(process);
				 }
			}
			Collections.sort(runningProcessList);
		}catch (SQLException e) {
			e.printStackTrace();
		}finally{
			ConnectionUtility.releaseConnectionResources(resultSet,preparedStatement, connection);
		}
		List<StreamingEntity> finalDrivers=new ArrayList<>();
		if(consumerName!=null){
			for(StreamingEntity driver:drivers){
				if(driver.getName().equalsIgnoreCase(consumerName)){
					finalDrivers.add(driver);
				}
			}
		}else{
			if(finalDrivers.size()==0){
				finalDrivers=drivers;
			}
		}
		if(isStreaming){
			responseEntity=new ResponseEntity<Object>(finalDrivers, HttpStatus.OK);
		}else{
			responseEntity=new ResponseEntity<Object>(runningProcessList, HttpStatus.OK);
		}
		return responseEntity;
	}
	
	/*
	 * this method will return the running job id(hadoop job id, spark job id) 
	 * based on search type.
	 */
	
	public String getJobId(Long entityId,String type) throws Exception {
		String jobId="";
		Connection connection = null;
		PreparedStatement preparedStatement=null;
		ResultSet resultSet=null;
		String query=ConnectionUtility.getSQlProperty("GET_PROJECT_JOB_ID");
		if(type.equalsIgnoreCase("ingestion")){
			query=ConnectionUtility.getSQlProperty("GET_INGESTION_JOB_ID");
		}
		try {
			connection=ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setLong(1,entityId);
			resultSet = preparedStatement.executeQuery();
			while(resultSet.next()){
				jobId=resultSet.getString(1);
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}finally{
			ConnectionUtility.releaseConnectionResources(resultSet,preparedStatement, connection);
		}
		return jobId;
	}
	
	/*
	 * this method will kill the running process by taking parameter job type and job id.
	 */

	public boolean killRunningJob(String searchType, String jobId, String userName) throws InterruptedException, IOException, SQLException {
		boolean status = false;
		ZDPDataAccessObjectImpl accessObjectImpl=new ZDPDataAccessObjectImpl();
		String[] args = new String[4];
		args[0] = ShellScriptExecutor.BASH;
		if (searchType.equalsIgnoreCase("ingestion")) {
			args[1] = System.getProperty("user.home") + "/zeas/Config/Terminate-hadoop-job.sh";
		} else if (searchType.equalsIgnoreCase("project")) {
			args[1] = System.getProperty("user.home") + "/zeas/Config/Terminate-oozie-job.sh";
		} else if (searchType.equalsIgnoreCase("streaming")) {
			args[1] =System.getProperty("user.home")+"/zeas/Config/Consumer-Stop.sh";
		}
		args[2] = jobId;
		args[3] = ConfigurationReader.getProperty("OOZIE_ENGINE");
		ProcessBuilder pb = new ProcessBuilder(args);
		pb.redirectErrorStream(true);
		Process p = null;
		p = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String text;
		if (searchType.equalsIgnoreCase("ingestion")) {
			while ((text = br.readLine()) != null) {
				if (text.contains("Killed job " + jobId)) {
					status = true;
					Thread.sleep(2000);
					break;
				}

			}
		} else if (searchType.equalsIgnoreCase("project")) {
			Thread.sleep(8000);
			status = true;
		} else if (searchType.equalsIgnoreCase("streaming")) {
			while ((text = br.readLine()) != null) {
				if (text.contains("Killed application " + jobId)) {
					status = true;
					Thread.sleep(2000);
					break;
				}
			}
			if(status==true){
				accessObjectImpl.addComponentRunStatus("", ZDPDaoConstant.STREAM_ACTIVITY, ZDPDaoConstant.JOB_TERMINATE, jobId, userName);
			}
		}

		return status;
	}
}

