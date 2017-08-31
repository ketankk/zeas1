package com.itc.taphius.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.itc.taphius.model.DataSourcer;
import com.itc.taphius.utility.DBUtility;

public class DataSourcerService {
	 
	 private Connection connection;
	 Properties prop = new Properties();

	 public DataSourcerService() {
	  connection = DBUtility.getConnection();
	 }
	  

	 public List<DataSourcer> getAllDataSourcer() {
	  List<DataSourcer> dataSourcerList = new ArrayList<DataSourcer>();

	  try {
		  Statement statement = connection.createStatement();
		 /* Properties prop = new Properties();
		  InputStream inputStream = DataSourcerService.class.getClassLoader().getResourceAsStream("/SQLEditor.properties");*/

		  //prop.load(inputStream);
		  String sQuery = DBUtility.getSQlProperty("LIST_SOURCER");
		  //String sQuery = prop.getProperty("LIST_SOURCER");
		  ResultSet rs = statement.executeQuery(sQuery);
		  while (rs.next()) {
			  DataSourcer dataSourcer = new DataSourcer();
			  dataSourcer.setId(rs.getInt("id"));
			  dataSourcer.setName(rs.getString("name"));
			  dataSourcer.setType(rs.getString("type"));
			  dataSourcer.setJsonblob(rs.getString("json_data"));
			  dataSourcer.setActive(rs.getBoolean("is_active"));
			  dataSourcer.setCreatedBy(rs.getString("created"));
			  dataSourcer.setLastUpdated(rs.getString("last_updated"));
			  dataSourcerList.add(dataSourcer);
		  }
	  } catch (SQLException e) {
	   e.printStackTrace();
	  }

	  return dataSourcerList;
	 }
	 
	 public void addSourcer(DataSourcer ds) {
	  try {
		  /*Properties prop = new Properties();
		  InputStream inputStream = DataSourcerService.class.getClassLoader().getResourceAsStream("/SQLEditor.properties");*/
		  //prop.load(inputStream);
		 // String sQuery = prop.getProperty("INSERT_SOURCER");
		  
		 /* String insertDataSourcer = "INSERT INTO ENTITY"
					+ "(name,type,json_data,is_active,created,last_updated) VALUES"
					+ "(?,?,?,?,?,?)";*/
		  
	   String sQuery = DBUtility.getSQlProperty("INSERT_SOURCER");
	   PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
	   preparedStatement.setString(1, ds.getName());
	   preparedStatement.setString(2, ds.getType());
	   preparedStatement.setString(3, ds.getJsonblob());
	   preparedStatement.setBoolean(4,ds.isActive());
	   preparedStatement.setString(5, ds.getCreatedBy());
	   preparedStatement.setString(6, ds.getLastUpdated());
		   
	   preparedStatement.executeUpdate();	   
	  } catch (SQLException e) {
	   e.printStackTrace();
	  }
	 
	 }
	 public void updateSourcer(DataSourcer ds,Integer dataSourceId) {
		 try {
			 //  String updateDataSourcer = "UPDATE  ENTITY SET  NAME =  ? ,TYPE =  ? ,  JSON_DATA =  ?,IS_ACTIVE =?,  CREATED =  ?,LAST_UPDATED =  ? WHERE ID = ? LIMIT 1 ";	  
			 // prop.load(inputStream);
			 //  String sQuery = prop.getProperty("UPDATE_SOURCER");
			 String sQuery = DBUtility.getSQlProperty("UPDATE_SOURCER");
			 PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			 preparedStatement.setString(1, ds.getName());
			 preparedStatement.setString(2, ds.getType());
			 preparedStatement.setString(3, ds.getJsonblob());
			 preparedStatement.setBoolean(4,ds.isActive());
			 preparedStatement.setString(5, ds.getCreatedBy());
			 preparedStatement.setString(6, ds.getLastUpdated());
			 preparedStatement.setInt(7,dataSourceId);

			 preparedStatement.executeUpdate();	   
		 } catch (SQLException e) {
		   e.printStackTrace();
		  }
		 
		 }
	 public DataSourcer getDataSourcerById(Integer id) {
		 DataSourcer dataSourcer = new DataSourcer();
		 
		
		//   String getDataSourcerById ="SELECT * FROM ENTITY WHERE ID = ? ";
		  try {
			//  prop.load(inputStream);
			//  String sQuery = prop.getProperty("SELECT_SOURCER_BY_ID");
			   
		   String sQuery = DBUtility.getSQlProperty("SELECT_SOURCER_BY_ID");
		   PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
		   preparedStatement.setInt(1,id);
		   ResultSet rs = preparedStatement.executeQuery();
		   while (rs.next()) {	
			   dataSourcer.setId(rs.getInt("id"));
			   dataSourcer.setName(rs.getString("name"));
			   dataSourcer.setType(rs.getString("type"));
			   dataSourcer.setJsonblob(rs.getString("json_data"));
			   dataSourcer.setActive(rs.getBoolean("is_active"));
			   dataSourcer.setCreatedBy(rs.getString("created"));
			   dataSourcer.setLastUpdated(rs.getString("last_updated"));
		   }
		  } catch (SQLException e) {
		   e.printStackTrace();
		  }
		 
		  return dataSourcer;
		 }
	 
	 public void deleteDataSourcer(Integer id) {
		 //String deleteSourcer ="DELETE FROM ENTITY WHERE ID = ? ";	 
		 try {

			 //String sQuery = prop.getProperty("DELETE_SOURCER");
			 String sQuery = DBUtility.getSQlProperty("DELETE_SOURCER");
			 PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			 preparedStatement.setInt(1,id);
			 preparedStatement.executeUpdate();
		
		 } catch (SQLException e) {
			 e.printStackTrace();
		 }

	 }

	}