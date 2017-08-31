package com.itc.zeas.project.model;

import com.zdp.dao.Component;
import lombok.Data;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ZDPProjectHistory implements Component {

	// this variable is exactly matches  with project table column
	private long id; //
	private long project_id;
	private int version;
	private String oozie_id;
	private String run_mode;
	private Timestamp start_time;
	private Timestamp end_time;
	private String created_by;
	private String status;
	private String run_details;
	
	
	public ZDPProjectHistory(){
		
	}
	
	public ZDPProjectHistory(long id, long project_id, int version,
			String run_mode, Timestamp start_time, Timestamp end_time,
			String created_by, String status, String run_details,String oozie_id) {
		this.id = id;
		this.project_id = project_id;
		this.version = version;
		this.run_mode = run_mode;
		//this.start_time = start_time;
		//this.end_time = end_time;
		this.created_by = created_by;
		this.status = status;
		this.run_details = run_details;
		this.oozie_id=oozie_id;
	}
	
	/*
	 * return the column and their datatype
	 */
	public Map<String, String> getColumnNameAndType() {
		
		Map<String, String> columnAndType = new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
			//if(!(f.getName().equals("id")))
			if(!(f.getName().equals("start_time") || f.getName().equals("end_time")))
			 columnAndType.put(f.getName(), f.getType().getSimpleName());
		}

		return columnAndType;
	}

	public Map<String, String> getColumnNameAndValue() {
		
		Map<String, String> columnNameAndValue= new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
			try {
				System.out.println(f.getName()+" value :"+f.get(this));
				String key=f.getName();
				Object val=f.get(this);
				if(!(f.getName().equals("start_time") || f.getName().equals("end_time"))){

					if (val != null) {
						columnNameAndValue.put(f.getName(), val.toString());
					} else {
						columnNameAndValue.put(key, "");
					}
			//	}
				}
			} catch (IllegalArgumentException |IllegalAccessException e) {
				e.printStackTrace();
			}
			
		}

		return columnNameAndValue;
	}


}
