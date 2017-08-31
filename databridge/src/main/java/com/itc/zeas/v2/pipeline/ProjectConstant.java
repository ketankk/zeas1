package com.itc.zeas.v2.pipeline;

public interface ProjectConstant {
	
	String COLUMN_SELECTOR_TRANSFORMATION="Column Filter";
	String COMPARE_MODEL_TRANSFORMATION="Compare Model";
	String TRAIN_MODEL_TRANSFORMATION="Train";
	String TEST_MODEL_TRANSFORMATION="Test";
	String CLEAN_MISSING_DATA_TRANSFORMATION="Clean Missing Data";
	String HIVE_TRANSFORMATION = "HIVE";
	String PIG_TRANSFORMATION = "PIG";
	String JOIN_TRANSFORMATION = "Join";
	String PARTITION_TRANSFORMATION="Partition";
	String SUBSET_TRANSFORMATION="Subset";
	String JOIN_NODE = "JoinNode";
	String MAPRED_TRANSFORMATION = "Mapreduce";
	String GROUP_BY_TRANSFORMATION = "Group By";
	//read intermediate output location
	String HDFS_TRANSF_OUTPUT_LOCATION="/tmp/zeas";
	
	//oozie job status constant
	
	//Machine Learning algorithms
	String ML_LENEAR_REGRESSION = "Linear Regression";
	String ML_MULTICLASS = "MultiClass Logistic Regression";
	
	//SUCCEEDED , KILLED , FAILED SUSPENDED
	String OOZIE_JOB_STATUS_SUCCEEDED="SUCCEEDED";
	String OOZIE_JOB_STATUS_KILLED="KILLED";
	String OOZIE_JOB_STATUS_FAILED="FAILED";
	String OOZIE_JOB_STATUS_RUNNING="RUNNING";
	String OOZIE_JOB_STATUS_SUSPENDED="SUSPENDED";
	//String OOZIE_JOB_STATUS_SUSPENDED="SUSPENDED";
	String START = "start";
	String FORK = "fork";
	String JOIN = "join";
	String PATH = "path";
	String OK = "ok";

	String TRANSFORMATION_JAR_PATH = "/user/zeas/Transformation.jar";
	String ML_LIB_JAR_PATH = "/user/zeas/MachineLearningLib.jar";
}
