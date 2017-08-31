package com.itc.taphius.utility;

import org.json.JSONObject;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.model.Entity;

/*
 * This class contains common utility functions
 */
public class CommonUtils {

	private int schId;
	private String format;
	private String schema;
	private String dataSet;
	private String frequency;
	
	String jsonData;
	Entity entityObj;
	EntityManager entity = new EntityManager();
	JSONObject schJsonObj = new JSONObject();

	/**
	 * Method used for getting source type using scheduler name from database
	 * @param schName
	 * @return sourceType
	 */
	public String getSourceType(String schName) {
		
		// Gets the entity object by scheduler name.
		entityObj = entity.getEntityByName(schName);
		schId  = entityObj.getId();
		
		// Retrieve json blob from entity. from JSON blob gets data source and data set details.
		jsonData = entityObj.getJsonblob();
		schJsonObj = new JSONObject(jsonData);
		String sourceType = null;

		try {
			String dataSource = schJsonObj.getString("dataSource");
			dataSet = schJsonObj.getString("destinationDataset");
			frequency = schJsonObj.getString("frequency");
			
			// Retrieve json blob from entity using data source field.
			entityObj = entity.getEntityByName(dataSource);
			jsonData = entityObj.getJsonblob();
			JSONObject DbJsonObj = new JSONObject(jsonData);

			// Get source type and schema name.
			sourceType = DbJsonObj.getString("sourcerType");
		    format = DbJsonObj.getString("format");
		    schema = DbJsonObj.getString("schema");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sourceType;
	}
	
	/**
	 * Method used for returning scheduler id
	 * @return schId
	 */
	public int getSchedularId() {
		return schId;
	}
	
	/**
	 * Method used for returning file format 
	 * @return format
	 */
	public String getFileFormat() {
		return format;
	}
	
	/**
	 * Method used for returning schema name 
	 * @return schema name
	 */
	public String getSchemaName() {
		return schema;
	}
	
	/**
	 * Method used for returning destination data set 
	 * @return dataset name
	 */
	public String getDestDataSet() {
		return dataSet;
	}
	
	/**
	 * Method used for returning frequency 
	 * @return frequency
	 */
	public String getFrequency() {
		return frequency;
	}
	
	/**
	 * Method used for getting source files 
	 * @return frequency
	 */	
	public String getSourceFile(String schName) {
		entityObj = entity.getEntityByName(schName);
		// Retrieve json blob from entity. from JSON blob gets data source and
		// data set details.
		jsonData = entityObj.getJsonblob();
		schJsonObj = new JSONObject(jsonData);
		String location = null;

		try {
			
			// Retrieve json blob from entity using data source field.
			String dataSource = schJsonObj.getString("dataSource");
			entityObj = entity.getEntityByName(dataSource);
			jsonData = entityObj.getJsonblob();
			JSONObject DbJsonObj = new JSONObject(jsonData);

			// get location
			location = DbJsonObj.getString("location");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return location;
	}
}
