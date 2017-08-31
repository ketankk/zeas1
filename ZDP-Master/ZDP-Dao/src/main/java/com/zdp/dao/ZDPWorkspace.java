package com.zdp.dao;

import lombok.Data;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ZDPWorkspace {
	
	private long id; //
	private String name;
	private Timestamp created;
	private String created_by;
	
	public ZDPWorkspace(long id, String name, Timestamp created,
			String created_by) {
		super();
		this.id = id;
		this.name = name;
		this.created = created;
		this.created_by = created_by;
	}
	
	/*
	 * return the column and their datatype
	 */
	public Map<String, String> getColumnNameAndType() {
		
		Map<String, String> columnAndType = new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
			 if(!(f.getName().equals("id")))
			 columnAndType.put(f.getName(), f.getType().getSimpleName());
		}

		return columnAndType;
	}

	public Map<String, String> getColumnNameAndValue() {
		
		Map<String, String> columnNameAndValue= new LinkedHashMap<String, String>();
		//Map<String, String> columnAndType = new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
			try {
				System.out.println(f.getName()+" value :"+f.get(this));
				String key=f.getName();
				Object val=f.get(this);
				if (!(f.getName().equals("id") || f.getName().equals(
						"columnNameAndValue"))) {
					if (val != null) {
						columnNameAndValue.put(f.getName(), val.toString());
					} else {
						columnNameAndValue.put(key, "");
					}
				}
			} catch (IllegalArgumentException |IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		return columnNameAndValue;
	}

	


}
