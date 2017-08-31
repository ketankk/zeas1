package com.taphius.databridge.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSource;

public class DataSourceMapper implements RowMapper<DataSource>{
    
    private static final Logger logger = LoggerFactory.getLogger(DataSourceMapper.class); 

	@Override
	public DataSource mapRow(ResultSet rs, int rowNum) throws SQLException {
		logger.debug("DataSource blob is =="+rs.getString("JSON_DATA"));
		DataSource ds = (new DataSourcerConfigDetails<DataSource>(DataSource.class)).getDSConfigDetails(rs.getString("JSON_DATA"));
		return ds;
	}

}
