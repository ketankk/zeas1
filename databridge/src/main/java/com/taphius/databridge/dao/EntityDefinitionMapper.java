package com.taphius.databridge.dao;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSourcerAttributes;
import com.itc.zeas.utility.DBUtility;



public class EntityDefinitionMapper implements RowMapper<DataSourcerAttributes>{
    
    private static final Logger logger = LoggerFactory.getLogger(EntityDefinitionMapper.class); 
	
	private DataSourcerConfigDetails<DataSourcerAttributes> parser;
	/**
	 * JDBC template used for querying DB
	 */
	private JdbcTemplate jdbcTemplate;
	
	public EntityDefinitionMapper(DataSourcerConfigDetails<DataSourcerAttributes> c, JdbcTemplate template){
		this.parser = c;
		jdbcTemplate =  template;
	}

	@Override
	public DataSourcerAttributes mapRow(ResultSet rs, int rowNum)
			throws SQLException {
		DataSourcerAttributes ds = null;
		
		try{
		    logger.debug("Ingestion blob is =="+rs.getString("JSON_DATA"));
		ds = parser.getDSConfigDetails(rs.getString("JSON_DATA"));
		ds.setDataSet(jdbcTemplate.queryForObject(DBUtility.getSQlProperty("GET_JSON_FOR_ENTITY"),  new Object[]{ds.getDestinationDataset()},new DataSetMapper()));
		ds.setDataSrc(jdbcTemplate.queryForObject(DBUtility.getSQlProperty("GET_JSON_FOR_ENTITY"),  new Object[]{ds.getDataSource()},new DataSourceMapper()));
		}catch(IOException ioE){
			logger.error("Exception accessing result set - "+ioE.getMessage());
		}
		return ds;
	}

	public DataSourcerConfigDetails<DataSourcerAttributes> getParser() {
		return parser;
	}

	public void setParser(DataSourcerConfigDetails<DataSourcerAttributes> parser) {
		this.parser = parser;
	}
	
	

}
