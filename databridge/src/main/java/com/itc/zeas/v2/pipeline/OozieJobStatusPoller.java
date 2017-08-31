package com.itc.zeas.v2.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taphius.databridge.dao.IngestionLogDAO;
import com.itc.zeas.project.model.ProjectEntity;
import com.zdp.dao.SearchCriteriaEnum;
import com.itc.zeas.project.model.SearchCriterion;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.project.extras.ZDPDaoConstant;

public class OozieJobStatusPoller implements Runnable {
	/**
	 * Oozie workflow id
	 */
	private String oozie_wf_id;
	/**
	 * Project run id - unique for each project RUN
	 */
	//private long project_run_id;
	boolean isRunning=true;
	private ZDPDataAccessObject accessObject;
	
	//action status cache
	private Map<String,String> actionsCacheStatus;
	private Map<String,String> jobsCacheStatus;
	private boolean isInsertedRunning=false;
	private boolean isInsertedComplete=false;
	private boolean isInsertedSusPend;
	
	public OozieJobStatusPoller(String workflow_id){
		this.oozie_wf_id = workflow_id;
		//this.project_run_id = project_id;
		accessObject= new ZDPDataAccessObjectImpl();
		actionsCacheStatus= new HashMap<String, String>();
		jobsCacheStatus=new HashMap<String, String>();
		isInsertedRunning=false;
		isInsertedComplete=false;
		isInsertedSusPend=false;
	}

	@Override
	public void run() {
		
		while(isRunning){
			checkJobStatus();
		}

	}
	
	private void checkJobStatus(){
		
		boolean isActionFinished=false;
		try {
			//reading job workflow
			List<Map<String,String>> wfJobResults=accessObject.getWFJobDeatails(oozie_wf_id);
			//update project history status..
			for(Map<String,String> job :wfJobResults) {
				String jobId=job.get("id");
				String jobStatus=job.get("status");
				if(!(jobsCacheStatus.get(jobId) !=null && jobsCacheStatus.get(jobId).equalsIgnoreCase(jobStatus))){
					//update module history
					Map<String,String> projectHistoryColumns=new HashMap<>();
					projectHistoryColumns.put("status", jobStatus);
					if(job.get("start_time") !=null)
					 projectHistoryColumns.put("start_time", job.get("start_time"));
					List<SearchCriterion> criterionlist= new ArrayList<>();
					SearchCriterion criterion= new  SearchCriterion("oozie_id", oozie_wf_id, SearchCriteriaEnum.EQUALS);
					criterionlist.add(criterion);
					Boolean isJobUpdated=accessObject.updateObject("project_history", projectHistoryColumns, criterionlist);
					System.out.println("update job :"+isJobUpdated);
					jobsCacheStatus.put(jobId, jobStatus);
					//add update cache value
				}
				
				//if(getWFJobStatusList().contains(jobStatus)) {
					
					//update the final project history action status
					Map<String,String> projectHistoryColumns=new HashMap<>();
					projectHistoryColumns.put("status", jobStatus);
					if(job.get("end_time") !=null )
						projectHistoryColumns.put("end_time", job.get("end_time"));
					//projectHistoryColumns.put("end_time", job.get("end_time"));
					List<SearchCriterion> criterionlist= new ArrayList<>();
					SearchCriterion criterion= new  SearchCriterion("oozie_id", oozie_wf_id, SearchCriteriaEnum.EQUALS);
					criterionlist.add(criterion);
					Boolean isJobUpdated=accessObject.updateObject("project_history", projectHistoryColumns, criterionlist);
					//added for notifiction message for project run
					try {
						List<SearchCriterion> crPrRunList = new ArrayList<>();
						SearchCriterion crPrRun = new SearchCriterion("oozie_id",oozie_wf_id,SearchCriteriaEnum.EQUALS);
						crPrRunList.add(crPrRun);
						ProjectEntity projectRunProjectEntity = accessObject.findExactObject(ZDPDaoConstant.ZDP_PROJECT_HISTORY, crPrRunList);
						Long prId=0l;
						String jobSt="";
						if(projectRunProjectEntity !=null && projectRunProjectEntity.getId()>0 ){
							prId= projectRunProjectEntity.getProject_id();
							jobSt= projectRunProjectEntity.getStatus();
							List<SearchCriterion> crList = new ArrayList<>();
							SearchCriterion cr = new SearchCriterion("id",prId.toString(),SearchCriteriaEnum.EQUALS);
							crList.add(cr);
							ProjectEntity projectProjectEntity = accessObject.findExactObject(ZDPDaoConstant.ZDP_PROJECT_TABLE, crList);
							String prName="";
							if(projectProjectEntity !=null && projectProjectEntity.getId()>0 ){
								prName= projectProjectEntity.getName();
							}
							 IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
							String actionUserName=ingestionLogDAO.getRunlogInfo(prName);
							List<String> users=accessObject.getUserListForGivenId(prId.toString(), ZDPDaoConstant.ZDP_PROJECT);
							if(!users.contains(actionUserName)){
								users.add(actionUserName);
							}
							if(users !=null && users.size()>0){
								String tempinfo="";
								switch(jobSt){
								case ProjectConstant.OOZIE_JOB_STATUS_RUNNING:
									if(!isInsertedRunning){
								accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_RUNNING, oozie_wf_id, actionUserName);
										isInsertedRunning=true;
											}
									break;
								case ProjectConstant.OOZIE_JOB_STATUS_FAILED:
									tempinfo="Project '"+prName+"' Failed";
									accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_KILL_FAIL, oozie_wf_id, actionUserName);
								accessObject.addActivitiesBatchForNewAPI(prName, tempinfo, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.FAIL_ACTIVITY, users,actionUserName);
									break;
								case ProjectConstant.OOZIE_JOB_STATUS_KILLED:
									tempinfo="Project '"+prName+"' Killed";
									accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_TERMINATE, oozie_wf_id, actionUserName);
								accessObject.addActivitiesBatchForNewAPI(prName, tempinfo, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_TERMINATE, users,actionUserName);
									break;
								case ProjectConstant.OOZIE_JOB_STATUS_SUCCEEDED:
									tempinfo="Project '"+prName+"' succeeded";
									if(!isInsertedComplete){
									accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_COMPLETE, oozie_wf_id, actionUserName);
								accessObject.addActivitiesBatchForNewAPI(prName, tempinfo, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.SUCCESS_ACTIVITY, users,actionUserName);
									isInsertedComplete=true;
									}
									break;
								case ProjectConstant.OOZIE_JOB_STATUS_SUSPENDED:
									tempinfo="Project '"+prName+"' Suspended";
									if(!isInsertedSusPend){
									accessObject.addComponentRunStatus(prName, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.JOB_KILL_FAIL, oozie_wf_id, actionUserName);
								accessObject.addActivitiesBatchForNewAPI(prName, tempinfo, ZDPDaoConstant.PROJECT_ACTIVITY, ZDPDaoConstant.FAIL_ACTIVITY, users,actionUserName);
								isInsertedSusPend=true;
									}
									break;
									default:
										break;
								
								}
							}
						}
						
					}catch(Exception e){
						System.out.println(e.toString());
					}
					
					//end
					System.out.println("update job :"+isJobUpdated);
					//update the final module history action status
					List<Map<String,String>> actionResults=accessObject.getWFActionsDeatails(oozie_wf_id);
					//update the intermediate  module history status
					for(Map<String,String> action :actionResults) {
						String actionId=action.get("id");
						String actionStatus=action.get("status");
						//if(!(actionsCacheStatus.get(actionId).equalsIgnoreCase(actionStatus))){
							//update module history
							String[] tempArr=actionId.split("@");
							String tempStr=tempArr[1];
							tempStr=tempStr.replace("m_", "");
							tempArr[1]=tempStr;
							String moduleId="";
							String version="";
							if(tempArr[1].contains("-")){
							 moduleId=tempArr[1].split("-")[0];
							 version=tempArr[1].split("-")[1];
							Map<String,String> moduleHistoryColumns=new HashMap<>();
							moduleHistoryColumns.put("status", actionStatus);
							if(action.get("end_time") !=null )
							  moduleHistoryColumns.put("end_time", action.get("end_time"));
							List<SearchCriterion> criterionMlist= new ArrayList<>();
							SearchCriterion ct1= new  SearchCriterion("oozie_id", oozie_wf_id, SearchCriteriaEnum.EQUALS);
							SearchCriterion ct2= new  SearchCriterion("module_id", moduleId, SearchCriteriaEnum.EQUALS);
							SearchCriterion ct3= new  SearchCriterion("version", version, SearchCriteriaEnum.EQUALS);
							criterionMlist.add(ct1);
							criterionMlist.add(ct2);
							criterionMlist.add(ct3);
							Boolean isActionUpdated=accessObject.updateObject("module_history", moduleHistoryColumns, criterionMlist);
							System.out.println("updated final module.."+actionId+" status:"+actionStatus);
							}
							//add update cache value
						//}
					}
					if(getWFJobStatusList().contains(jobStatus)){
					isActionFinished=true;
					isRunning=false;
					System.out.println("job has been finished..");
					}
				//}
				
			}
			//end
			if(!isActionFinished) {
				List<Map<String,String>> actionResults=accessObject.getWFActionsDeatails(oozie_wf_id);
			//update the intermediate  module history status
				for(Map<String,String> action :actionResults) {
					String actionId=action.get("id");
					String actionStatus=action.get("status");
					if(!(actionsCacheStatus.get(actionId) !=null && actionsCacheStatus.get(actionId).equalsIgnoreCase(actionStatus))){
					//update module history
						String[] tempArr=actionId.split("@");
						String tempStr=tempArr[1];
						tempStr=tempStr.replace("m_", "");
						tempArr[1]=tempStr;
						String moduleId="";
						String version="";
						if(tempArr[1].contains("-")) {
						 moduleId=tempArr[1].split("-")[0];
						 version=tempArr[1].split("-")[1];
						Map<String,String> moduleHistoryColumns=new HashMap<>();
						moduleHistoryColumns.put("status", actionStatus);
						if(action.get("start_time") !=null)
						 moduleHistoryColumns.put("start_time", action.get("start_time"));
						if(actionStatus.equalsIgnoreCase("OK") || actionStatus.equalsIgnoreCase("ERROR"))
						 moduleHistoryColumns.put("end_time", action.get("end_time"));
						List<SearchCriterion> criterionMlist= new ArrayList<>();
						SearchCriterion ct1= new  SearchCriterion("oozie_id", oozie_wf_id, SearchCriteriaEnum.EQUALS);
						SearchCriterion ct2= new  SearchCriterion("module_id", moduleId, SearchCriteriaEnum.EQUALS);
						SearchCriterion ct3= new  SearchCriterion("version", version, SearchCriteriaEnum.EQUALS);
						criterionMlist.add(ct1);
						criterionMlist.add(ct2);
						criterionMlist.add(ct3);
						Boolean isActionUpdated=accessObject.updateObject("module_history", moduleHistoryColumns, criterionMlist);
						System.out.println("updated intermediate module.."+actionId+" status:"+actionStatus);
						//add update cache value
						actionsCacheStatus.put(actionId, actionStatus);
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	/*public long getProject_run_id() {
		return project_run_id;
	}

	public void setProject_run_id(long project_run_id) {
		this.project_run_id = project_run_id;
	}*/

	public String getOozie_wf_id() {
		return oozie_wf_id;
	}

	public void setOozie_wf_id(String oozie_wf_id) {
		this.oozie_wf_id = oozie_wf_id;
	}
	
	public List<String> getWFJobStatusList(){
		
		List<String> jobStatusList= new ArrayList<String>();
		jobStatusList.add(ProjectConstant.OOZIE_JOB_STATUS_SUCCEEDED);
		jobStatusList.add(ProjectConstant.OOZIE_JOB_STATUS_KILLED);
		jobStatusList.add(ProjectConstant.OOZIE_JOB_STATUS_FAILED);
		jobStatusList.add(ProjectConstant.OOZIE_JOB_STATUS_SUSPENDED);
		return jobStatusList;
	}

}
