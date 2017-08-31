package com.itc.zeas.project.extras;


public interface QueryConstants {

	
	// Making query related constant
	String QUERY_AND_TYPE = " AND ";
	String QUERY_OR_TYPE = " OR ";
	String QUERY_AND_CLOSE_TYPE = "AND )";
	String QUERY_OR_CLOSE_TYPE = "OR )";
	String QUERY_AND_OPEN_TYPE = "AND (";
	String QUERY_OR_OPEN_TYPE = "OR (";
	String QUERY_OPEN_TYPE="(";
	
	// key for query and value
	String columnQuery="query";
	String columnValues="values";
	
	String LATEST_PROJECT="select p.id as id,p.name as name,p.design as design,p.version as version,p.created as created,p.created_By as created_By,p.workspace_name as workspace_name from project p," +
			"(select name,max(version) v1 from project group by name,created_By) as p2 where p.name=p2.name and p.version=p2.v1 ";
	// currently there is no scenario to use this query, if need please update query
	String LATEST_MODULE="select p.id as id,p.prop as prop,p.version as version,p.comp_type as comp_type,p.proj_id as proj_id,p.created_at as created_at,p.user as user from module p," +
			"(select comp_type,proj_id,max(version) as v1 from module where proj_id=? and comp_type=? group by comp_type ) p2" +
			"  where p.proj_id=p2.proj_id and p.version=p2.v1 and p.comp_type=p2.comp_type";
	String SEQUENCE_QUERY=" insert into sequenceid() values()";
	String PROJECT_HISTORY_ID_QUERY="insert into projecthistoryid() values()";
	
}




