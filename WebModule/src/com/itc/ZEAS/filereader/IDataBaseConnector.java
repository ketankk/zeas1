package com.itc.zeas.filereader;

import java.sql.Connection;
import java.sql.SQLException;

import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasSQLException;

/*
 * This interface helps to connect to database and getting the connection object.
 * Also gets queries needed for execution  
 */
public interface IDataBaseConnector {
	
	public Connection getDataBaseConnection() throws ZeasSQLException, ZeasException; 
	
}
