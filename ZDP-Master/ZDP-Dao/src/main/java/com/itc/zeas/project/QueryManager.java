package com.itc.zeas.project;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONObject;

import com.itc.zeas.utility.connection.ConnectionUtility;

/**
 * @author 11786
 * 
 */

public class QueryManager {

	Properties prop = new Properties();


	/**
	 * This method is to used to get output of dynamic select query
	 * 
	 *            sQuery
	 * @return List<JSONObject>
	 * @throws SQLException
	 */
	public List<JSONObject> getResult(String sQuery) throws SQLException {
		List<JSONObject> queryOutput = new ArrayList<JSONObject>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			 rs = preparedStatement.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			String key, value;
			while (rs.next()) {
				JSONObject jsonObject = new JSONObject();
				for (int i = 1; i <= columnCount; i++) {
					key = rsmd.getColumnName(i).toString().toUpperCase();
					value = rs.getObject(key).toString();
					jsonObject.put(key, value);
				}
				queryOutput.add(jsonObject);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			List<JSONObject> errorOutput = new ArrayList<JSONObject>();
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("Error", e.getMessage());
			errorOutput.add(jsonObject);
			return errorOutput;
		}finally {
			if (rs != null) {
				rs.close();
			}
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}

		return queryOutput;
	}

}