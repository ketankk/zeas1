package com.zdp.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.itc.zeas.ingestion.model.ZDPRunLogDetails;
import com.itc.zeas.ingestion.model.ZDPScheduler;
import com.itc.zeas.project.model.ProjectEntity;
import com.itc.zeas.project.model.SearchCriterion;
import com.itc.zeas.model.ModuleSchema;
import com.itc.zeas.model.ProjectRunHistory;


public interface ZDPDataAccessObject {
	
	//add entity (transformation,stage,project)
	public ProjectEntity addEntity(ProjectEntity projectEntity);
	//update entity (transformation,stage,project)
	public ProjectEntity updateEntity(ProjectEntity projectEntity, String type, Integer id) ;
	//delete entity (transformation,stage,project)
	public boolean deleteEntity(String comp_type, List<SearchCriterion> criterions) throws Exception;
	// find the object for given type and finding criteria.
	public ProjectEntity findExactObject(String comp_type, List<SearchCriterion> criterions) throws Exception;
	//find the objects for given input
	public List<ProjectEntity> findObjects(String comp_type, List<SearchCriterion> criterions) throws Exception;
	
	//find the latest version objects for given input (schema type)
	public List<ProjectEntity> findLatestVersionProjects(String schemaType) throws Exception;
	//find the latest version objects for given input (entity needs values schema type, name,project id,) . it uses only for module.
	public ProjectEntity findLatestVersionObject(ProjectEntity projectEntity) throws Exception;
	// find the object for given type ,finding criteria and order by list and their type either desc or asd
	public List<Component> findObjects(String comp_type,List<SearchCriterion> criterions,List<String> orderedList ,String orderType) throws Exception;
	// find the column list for give type object and their id
	public List<ModuleSchema> getColumnNames(String comp_type, String id, String version) throws Exception;
	//update the object 
	public Boolean updateObject(String comp_type,Map<String,String> columnNameAndValues ,List<SearchCriterion> criterions) throws Exception;
	
	//gets oozie work flow job details
	public List<Map<String,String>> getWFJobDeatails(String oozieId) throws Exception;
	//get oozie actions details for for submmitted job
	public List<Map<String,String>> getWFActionsDeatails(String oozieId) throws Exception;
	
	//get schema details name and datatype which is seperated by :(colon)
	public List<ModuleSchema> getColumnAndDatatype(String schema) throws Exception;
	// get project  run history for given id.
	public List<ProjectRunHistory> getProjectRunHistoryInfo(String name, String id) throws SQLException;
	
	// insert use action information. map containg the information about user,scheduler/project name, id ,action type etc
	public boolean addUserAction(Map<String,String> actionInfo) throws SQLException;
	// gives user name for give ingestion or project run , it takes scheduler or project name.
	public String  getUserForAction(String name) throws SQLException;
	//Delete user action history of previously logged. It takes user .
	public boolean deleteUserAction(String userName) throws SQLException;
	// gets list of user for given dataset and project id which have permission.
	public List<String> getUserListForGivenId(String id, String type) throws Exception;
	
	// it is used for project export for given project entity. it generates json output at  given directory path location(e.g path/project_id_version.json). 
	public String exportProject(ProjectEntity projectEntity, String exportPath) throws Exception;
	//import project from exported file
	public String importProject(String filePath,String user) throws Exception;
	public List<String> getUsedProjectList(String dataSetName) throws Exception;
	//get all module history list for given project_id and version
	public List<ZDPModuleHistory> getModuleHistory(String project_run_Id,String projectId,String version) throws Exception;
	
	//insert runtime log details
	public boolean createRunLogDetail(String name,String type,String status,String logFileLocation,String created_by) throws Exception ;
	
	// get latest runlog object for given dataset and project
	public ZDPRunLogDetails getLatestRunLogDetail(String name, String userName) throws Exception;
	//add notification information for each user
	void addActivitiesBatchForNewAPI(String entityName, String statusInfo, String component, String operation,
			List<String> users, String action_user) throws SQLException;
	// add to component_execution table for various module
	void addComponentExecution(String entityName, String component, String users) throws SQLException;
	// add run status of components in comp_exce_status table.
	void addComponentRunStatus(String entityName, String componentType, String runStatus, String jobId, String userName)
			throws SQLException;
	//validate project name with existing name. It should be unique across user.
	Boolean isProjectExist(String projectName,String user) throws SQLException;
	
	public Map<Long,String> getProjectSchema(Long projectId) throws SQLException;
	
	public Map<String,String> getProjectOuputDetails(Long moduleId) throws SQLException;
	public List<String> getDataSetFromProject(Long projectId, Integer version);
	
	public ZDPScheduler getScheduler(final long project_id);
	public void persistScedulerDetail(final ZDPScheduler scheduler);
	public String getJSONFromEntity(String datasetName);
	public String getCSVFIlePath(final String datasetName); 
	public String getProjectTypeDetail(final int projectId) ;
	public String getdatasetIdByName(String datasetName);
}
