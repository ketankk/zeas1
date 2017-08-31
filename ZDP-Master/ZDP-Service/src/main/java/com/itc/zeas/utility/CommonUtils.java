package com.itc.zeas.utility;

import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.model.Entity;
import lombok.Data;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.SQLException;

/*
 * This class contains common utility functions
 */
@Data
public class CommonUtils {

	private int schedulerId;
	private String format;
	private String schema;
	private String dataSet;
	String dataSource;
	private String frequency;

	String jsonData;
	Entity entityObj;
	EntityManager entity = new EntityManager();
	JSONObject schJsonObj = new JSONObject();

	/**
	 * Method used for getting source type using scheduler name from database
	 * 
	 * @param schName
	 * @return sourceType
	 * @throws Exception 
	 * @throws SQLException 
	 */
	public String getSourceType(String schName) throws Exception {

		// Gets the entity object by scheduler name.
		entityObj = entity.getEntityByName(schName);
		schedulerId = (int) entityObj.getId();

		// Retrieve json blob from entity. from JSON blob gets data source and
		// data set details.
		jsonData = entityObj.getJsonblob();
		schJsonObj = new JSONObject(jsonData);
		String sourceType = null;

		try {
			dataSource= schJsonObj.getString("dataSource");
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
	 * Method used for getting source files
	 * 
	 * @return frequency
	 * @throws Exception 
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public String getSourceFile() throws Exception {
		try {
			entityObj = entity.getEntityByName(this.dataSource);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Retrieve json blob from entity. from JSON blob gets data source and
		// data source details.
		jsonData = entityObj.getJsonblob();
		schJsonObj = new JSONObject(jsonData);
		String location = schJsonObj.getString("location");;
		return location;
	}

	/**
	 * Retreives access token from http request
	 * 
	 * @param httpRequest
	 * @return
	 */
	public String extractAuthTokenFromRequest(HttpServletRequest httpRequest) {
		/* Get token from header */
		String authToken = httpRequest.getHeader("X-Auth-Token");

		/* If token not found get it from request parameter */
		if (authToken == null) {
			authToken = httpRequest.getParameter("token");
		}

		return authToken;
	}

	/**
	 * gives the user name for a given access token string
	 * 
	 * @param authToken
	 * @return
	 */
	public String getUserNameFromToken(String authToken) {
		if (null == authToken) {
			return null;
		}

		String[] parts = authToken.split(":");
		return parts[0];
	}

	/**
	 * Retrieves user name from HttpServletRequest
	 * 
	 * @param httpRequest
	 * @return user name
	 */
	public String extractUserNameFromRequest(HttpServletRequest httpRequest) {
		String authToken = extractAuthTokenFromRequest(httpRequest);
		String userName = getUserNameFromToken(authToken);
		return userName;
	}
}
