package com.itc.zeas.project.model;

import com.zdp.dao.Component;
import lombok.Data;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ZDPProject implements Component {


	// this variable is exactly matches  with project table column
	private long id; //
	private String name;
	private String design;
	private int version;
	private Timestamp created;
	private String created_by;
	private String workspace_name;
	
	public ZDPProject(){
		
	}
	
	public ZDPProject(long id, String name, String design, int version,
			Timestamp created_at, String user,String workspace_name) {
		this.id = id;
		this.name = name;
		this.design = design;
		this.version = version;
		this.created = created_at;
		this.created_by = user;
		this.workspace_name=workspace_name;
	}

	
	/*
	 * return the column and their datatype
	 */
	public Map<String, String> getColumnNameAndType() {
		
		Map<String, String> columnAndType = new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
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
					if (val != null) {
						columnNameAndValue.put(f.getName(), val.toString());
					} else {
						columnNameAndValue.put(key, "");
					}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		return columnNameAndValue;
	}


	
}
