package com.itc.taphius.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONObject;

import com.itc.taphius.utility.DBUtility;


/**
 * @author 11786
 *
 */

public class QueryManager {
	 
	 private Connection connection;
	 Properties prop = new Properties();

	 public QueryManager() {
	  connection = DBUtility.getConnection();
	 }
	  
	 /**
		 * This method is to used to get output of dynamic select query
		 * @param String sQuery
		 * @return List<JSONObject>
		 */ 
	public List<JSONObject> getResult(String sQuery) {	
		List<JSONObject> queryOutput = new ArrayList<JSONObject>();
		try{
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);		
			ResultSet rs = preparedStatement.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
		    int columnCount = rsmd.getColumnCount();
		    String key,value;
			while (rs.next()) {
				JSONObject jsonObject = new JSONObject();
			    for(int i = 1 ; i <= columnCount ; i++){			    	
				    	key = rsmd.getColumnName(i).toString().toUpperCase();
				        value = rs.getObject(key).toString();
				    	jsonObject.put(key,value);
			    }
			    queryOutput.add(jsonObject);
			}
		}catch (SQLException e) {
			e.printStackTrace();	
			List<JSONObject> errorOutput = new ArrayList<JSONObject>();
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("Error", e.getMessage());
			errorOutput.add(jsonObject);
			return errorOutput;
		}
		
		return queryOutput;
	}

	}