package com.taphius.databridge.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.itc.zeas.utility.connection.ConnectionUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSourcerAttributes;
import com.itc.zeas.utility.DBUtility;

public class EntityDefinationDAO {
    
    private static final Logger logger = LoggerFactory.getLogger(EntityDefinationDAO.class); 
	
	/**
	 * JDBC template used for querying DB
	 */
	private JdbcTemplate jdbcTemplate;
	
	private DataSourcerConfigDetails<DataSourcerAttributes> configDetails;
	
	public EntityDefinationDAO() {
		jdbcTemplate = new JdbcTemplate(DBUtility.getMySQLDataSource());
	}

	public JdbcTemplate getJdbcTemplate() {		
		return jdbcTemplate;
		
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public List<DataSourcerAttributes> getNewlyAddedIngestions(String entityType) throws Exception {
		logger.debug("Inside getNewlyAddedIngestions");
    	 List<DataSourcerAttributes> dsAttrs = new ArrayList<DataSourcerAttributes>();
		try {
			dsAttrs = jdbcTemplate.query(ConnectionUtility.getSQlProperty("LATEST_INGESTIONS"),
			                   new Object[]{entityType}, new EntityDefinitionMapper(configDetails, jdbcTemplate));
		} catch (DataAccessException | IOException e) {
		    logger.error("Exception accessing result set - "+e.getMessage());
		}
    	return dsAttrs;
		
	}
	
	public List<DataSourcerAttributes> getTotalIngestions(String entityType) throws Exception {
		logger.debug("Inside getTotalIngestions");
    	 List<DataSourcerAttributes> dsAttrs = new ArrayList<DataSourcerAttributes>();
		try {
			dsAttrs = jdbcTemplate.query( ConnectionUtility.getSQlProperty("TOTAL_INGESTIONS"),
			                   new Object[]{entityType}, new EntityDefinitionMapper(configDetails, jdbcTemplate));
		} catch (DataAccessException | IOException e) {
		    logger.error("Exception accessing result set - "+e.getMessage());
		}
    	return dsAttrs;
		
	}

	public DataSourcerConfigDetails<DataSourcerAttributes> getConfigDetails() {
		return configDetails;
	}

	public void setConfigDetails(DataSourcerConfigDetails<DataSourcerAttributes> configDetails) {
		this.configDetails = configDetails;
	}

}
