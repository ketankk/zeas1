package com.itc.zeas.project.extras;

public interface ZDPDaoConstant {
	
	String ZDP_PROJECT_TABLE="project";
	String ZDP_TEST="type";
	String ZDP_MODULE_TABLE="module";
	String ZDP_ENTITY_TABLE="entity";
	String ZDP_WORKSPACE="workspace";
	String ZDP_PROJECT_HISTORY="project_history";
	String ZDP_MODULE_HISTORY="module_history";
	String HISTORY="history";
	
	
	//hive temporary table which delete once determine the schema of hive
	String TEMPORART_HIVE_TABLE="hivevalidatetable";
	
	//ingestionruninfo table
	String ZDP_INGESTION_RUN_INFO="ingestion_run_info";
	String ZDP_INGESTION="ingestion";
	String ZDP_PROJECT="project";
	//Activity related constants 
	
		//operations constants
	String CREATE_ACTIVITY="CREATE";
	String UPDATE_ACTIVITY="UPDATE";
	String EXECUTE_ACTIVITY="EXECUTE";
	String INITIATE_ACTIVITY="START";
	String FAIL_ACTIVITY="FAIL";
	String DELETE_ACTIVITY="DELETE";
	String SUCCESS_ACTIVITY="SUCCESS";
	
	String FILE_COPYING="COPYING";
	String CHECKING_DATA_QUALITY="CHECKING DATA QUALITY";
	String JOB_SCHEDULE="SCHEDULING";
	String JOB_RUNNING="RUNNING";
	String JOB_COMPLETE="COMPLETE";
	String JOB_KILL_FAIL="FAIL";
	String JOB_TERMINATE="TERMINATE";
	String DATA_IMPORTING="IMPORTING";

	 String INGESTION_ACTIVITY="ingestion";
	 String PROJECT_ACTIVITY="project";
	 String STREAM_ACTIVITY="streaming";

}
