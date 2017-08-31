package com.itc.zeas.ingestion.automatic.rdbms;

import java.sql.Connection;

import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.exceptions.ZeasException;

/*
 * This interface helps to connect to database and getting the connection object.
 * Also gets queries needed for execution  
 */
public interface IDataBaseConnector {
	
	 Connection getDataBaseConnection() throws ZeasSQLException, ZeasException;
	
}
