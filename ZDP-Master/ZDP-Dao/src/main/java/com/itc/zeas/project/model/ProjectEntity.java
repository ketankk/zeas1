package com.itc.zeas.project.model;

import com.itc.zeas.profile.model.Entity;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class ProjectEntity extends Entity {

	private Timestamp created;
	private Timestamp updated;
	private String htmlPath;
	private long project_id;
	private String component_type;
	private String workspace_name;
	// added for project and module history
	private String run_mode;
	private String status;
	private String run_details;
	private Timestamp start_time;
	private Timestamp end_time;
	private long module_id;
	private String details;
	private long project_run_id;
	private String output_blob;
	private String oozie_id;;

	private String filename;
	private String md5;
	private long noofrecord;
	private String schemaname;
	/**
	 * This attribute is used for Hive Action.
	 * If output schema is defined (its defined in case of Hive with UDF)
	 * then this flag will be set to true.
	 */
	private boolean isOutputDefined;

	// added by deepak to provide permission with project information
	private int permissionLevel;
	private String exportLocation;




}
