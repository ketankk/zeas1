package com.itc.taphius.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.itc.taphius.model.Entity;
import com.itc.taphius.model.StreamDriver;
import com.itc.taphius.utility.DBUtility;

public class StreamDriverManager {

	private Connection connection;
	private Logger logger = Logger.getLogger(StreamDriverManager.class);
	Properties prop = new Properties();
	
	
	
	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws SQLException
	 */
	public void addEntity(Entity entity) {
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_ENTITY");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, entity.getName());
			preparedStatement.setString(2, entity.getType());
			preparedStatement.setString(3, entity.getJsonblob());
			// Currently sending '1' as isActive value
			preparedStatement.setBoolean(4, true);
			preparedStatement.setString(5, entity.getCreatedBy());
			preparedStatement.setString(6, entity.getUpdatedBy());
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws SQLException
	 */
	
	
	public StreamDriver getEntityByName(String name) {
		StreamDriver entity = new StreamDriver();
		try {
			String sQuery = DBUtility.getSQlProperty("GET_ENTITY_BY_NAME");
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			preparedStatement.setString(1, name);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				entity.setId(rs.getInt("id"));
				entity.setName(rs.getString("name"));
				entity.setType(rs.getString("type"));
				entity.setJsonblob(rs.getString("json_data"));
				entity.setActive(rs.getBoolean("is_active"));
				entity.setCreatedBy(rs.getString("created_by"));
				entity.setCreatedDate(rs.getTimestamp("created"));
				entity.setUpdatedBy(rs.getString("updated_by"));
				entity.setUpdatedDate(rs.getTimestamp("last_modified"));
				logger.debug("name:" + name);
				// logger.debug("entity date:" +
				// entity.getUpdatedTimestamp().getTime());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// finally {
		// closeConnection(connection);
		// }

		return entity;
	}
	
	
	public List<Entity> getStreamingEntity() {
		EntityManager emgr =new EntityManager();
		List<Entity> entities = emgr.getEntity("streaming");
		String sQuery = DBUtility.getSQlProperty("LIST_JOB_COUNT");
        
		Connection connection = DBUtility.getConnection();
		PreparedStatement preparedStatement=null;
		ResultSet rs = null;
		for (Entity entity : entities) {
			int count = 0;
			// find the count
			String jsonBlob=entity.getJsonblob();
			try {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(jsonBlob);
				JsonNode consumerName=rootNode.get("Consumer_Name");
				System.out.println("consumerNam:"+consumerName);
				try{
				preparedStatement = connection.prepareStatement(sQuery);
				preparedStatement.setString(1,consumerName.getTextValue() );
				preparedStatement.setString(2, "Running");
				 rs = preparedStatement.executeQuery();
				 if(rs.next()){
					 count=rs.getInt(1); 
				 }
				}catch(SQLException e){
					e.printStackTrace();
				}
				((ObjectNode)rootNode).put("count", count);
				jsonBlob=rootNode.toString();
				logger.debug("new jsonBlobstring with count:"+jsonBlob);
				entity.setJsonblob(jsonBlob);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return entities;
	}
	
	
	
	
	
	public void addStreamDriver(String appId,String consumer,String user){
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_STREAM_DRIVER");
			String status="RUNNING";
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			
			preparedStatement.setString(1, appId);
			preparedStatement.setString(2, consumer);
			preparedStatement.setString(3, status);
			preparedStatement.setString(4, user);
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}
	}
	
	
	public void updateStreamDriver(String driverId,String user) {
		try {


			String sQuery = DBUtility.getSQlProperty("UPDATE_STREAM_DRIVER");
			String status="STOPPED";
			
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			
			preparedStatement.setString(1, status);
			preparedStatement.setString(2, user);
			preparedStatement.setString(3, driverId);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}

	}
	
	
	public List<StreamDriver> getStreamDriver(String name,String status) {
		List<StreamDriver> drivers = new ArrayList<StreamDriver>();
		String sQuery=DBUtility.getSQlProperty("LIST_DRIVER");	
		try {
			if (name == null) {
				sQuery=DBUtility.getSQlProperty("LIST_RUNNING_DRIVER");	
			} 
			connection = DBUtility.getConnection();
			PreparedStatement preparedStatement = connection
					.prepareStatement(sQuery);
			
			preparedStatement.setString(1, status);
			if(name !=null)
			 preparedStatement.setString(2, name);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				
				StreamDriver driver=new StreamDriver();
				driver.setDriverId(rs.getString("driver_id"));
				driver.setConsumerName(rs.getString("consumer_name"));
				driver.setStartAt(rs.getTimestamp("start_at").toString());
				
				//driver.setStopAt(rs.getDate("stop_at").toString());
				driver.setStartBy(rs.getString("start_by"));
				//driver.setStopBy(rs.getString("stop_by"));
				
				System.out.println("Driver ......:"+driver.getDriverId());
				System.out.println("Start at**********:"+driver.getStartAt());
				drivers.add(driver);
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}

		return drivers;
	}
	
	private void closeConnection(Connection con) {
		return;
		/*
		 * try { if(null != con) con.close(); } catch (SQLException e) {
		 * e.printStackTrace(); }
		 */

	}
	

}
