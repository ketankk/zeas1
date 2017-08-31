package com.zdp.dao;

import lombok.Data;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ZDPModule implements Component {

	// this variable is exactly matches  with module table column
	private long id;
	private String component_type;  //foreign key
	private String properties;
	private int version;
	private long project_id;  //foreign key
	private Timestamp created;
	private String created_by;
	
	public ZDPModule(){
		
	}
	
	public ZDPModule(long id, String comp_type, String prop, int version,
			long proj_id, Timestamp created_at, String user) {
		this.id = id;
		this.component_type = comp_type;
		this.properties = prop;
		this.version = version;
		this.project_id = proj_id;
		this.created = created_at;
		this.created_by = user;
	}


	/*
	 * return the column and their datatype
	 */
	public Map<String, String> getColumnNameAndType() {
		
		Map<String, String> columnAndType = new LinkedHashMap<String, String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		String fkey_comp_type="";
		String fkey_proj_id="";
		for (Field f : fields) {
			
			//for foreign key
			
			try {
				System.out.println(f.getName()+" value :"+f.get(this));
				String key=f.getName();
				Object val=f.get(this);
//				if (!(f.getName().equals("id") || f.getName().equals(
//						"columnNameAndValue"))) {
					if (key.equals("component_type") && val !=null) {
						fkey_comp_type= val.toString();
					} 
					if (key.equals("project_id")  && val !=null) {
						fkey_proj_id= val.toString();
					} 
			//	}
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//end
			//if(!(f.getName().equals("id") )) {
				if(f.getName().equals("component_type")  ) {
					if(!fkey_comp_type.isEmpty())
						columnAndType.put(f.getName(), f.getType().getSimpleName());
				}
				else if(f.getName().equals("project_id")  ) {
					if(!fkey_proj_id.isEmpty() && !fkey_proj_id.equals("0"))
					 columnAndType.put(f.getName(), f.getType().getSimpleName());
				}
				else{
					columnAndType.put(f.getName(), f.getType().getSimpleName());
				}
			//}
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
				//if (!(f.getName().equals("id") )) {
					if (val != null) {
						columnNameAndValue.put(f.getName(), val.toString());
					} else {
						columnNameAndValue.put(key, "");
					}
				//}
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		return columnNameAndValue;
	}


}
