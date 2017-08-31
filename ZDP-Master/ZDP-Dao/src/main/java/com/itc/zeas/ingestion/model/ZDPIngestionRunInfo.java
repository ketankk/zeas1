package com.itc.zeas.ingestion.model;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

public class ZDPIngestionRunInfo {

	// this variable is exactly matches with IngestionRunInfo table column
	private long id; //
	private String md5;
	private String filename;
	private String schemaName;
	private long noofrecord;
	private Timestamp created;
	private String created_by;


	public ZDPIngestionRunInfo() {

	}

	public ZDPIngestionRunInfo(long id, String md5, String filename,
			String shcemaName, long noOfrecord, Timestamp created,
			String created_by) {
		this.id = id;
		this.md5 = md5;
		this.filename = filename;
		this.schemaName = shcemaName;
		this.noofrecord = noOfrecord;
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
			// if(!(f.getName().equals("id")))
			columnAndType.put(f.getName(), f.getType().getSimpleName());
		}

		return columnAndType;
	}

	public Map<String, String> getColumnNameAndValue() {

		Map<String, String> columnNameAndValue = new LinkedHashMap<String, String>();
		// Map<String, String> columnAndType = new LinkedHashMap<String,
		// String>();
		Class<?> objClass = this.getClass();
		Field[] fields = objClass.getDeclaredFields();
		for (Field f : fields) {
			try {
				System.out.println(f.getName() + " value :" + f.get(this));
				String key = f.getName();
				Object val = f.get(this);
				// if (!(f.getName().equals("id") || f.getName().equals(
				// "columnNameAndValue"))) {
				if (val != null) {
					columnNameAndValue.put(f.getName(), val.toString());
				} else {
					columnNameAndValue.put(key, "");
				}
				// }
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


	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getShcemaName() {
		return schemaName;
	}

	public void setShcemaName(String shcemaName) {
		this.schemaName = shcemaName;
	}

	public long getNoOfrecord() {
		return noofrecord;
	}

	public void setNoOfrecord(long noOfrecord) {
		this.noofrecord = noOfrecord;
	}

	public Timestamp getCreated() {
		return created;
	}

	public void setCreated(Timestamp created) {
		this.created = created;
	}

	public String getCreated_by() {
		return created_by;
	}

	public void setCreated_by(String created_by) {
		this.created_by = created_by;
	}
}
