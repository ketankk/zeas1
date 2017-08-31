package com.taphius.databridge.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSet;

public class DataSetMapper implements RowMapper<DataSet>{
    
    private static final Logger logger = LoggerFactory.getLogger(DataSetMapper.class); 

	@Override
	public DataSet mapRow(ResultSet result, int rowNum) throws SQLException {
		logger.debug("DataSet blob is =="+result.getString("JSON_DATA"));				
		return (new DataSourcerConfigDetails<DataSet>(DataSet.class)).getDSConfigDetails(result.getString("JSON_DATA"));
	}

}
