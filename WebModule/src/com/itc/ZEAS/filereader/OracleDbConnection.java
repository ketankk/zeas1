package com.itc.zeas.filereader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasFileNotFoundException;
import com.itc.zeas.exception.ZeasSQLException;

/*
 * This class connects to oracle database and returns the connection object.
 */
public class OracleDbConnection implements IDataBaseConnector {
	
	Logger logger= Logger.getLogger("OracleDbConnection");
	
	private ExtendedDetails dbInfo;

	public OracleDbConnection(ExtendedDetails dbInfo) {	
		this.dbInfo = dbInfo;
	}

	/**
	 * Method for getting database connection
	 * @return connObject
	 * @throws ZeasSQLException 
	 */
	public Connection getDataBaseConnection() throws ZeasException {	
		Connection connObject = null;
		
		try {
			
			String hostname = dbInfo.getHostName();
			String dbName = dbInfo.getDbName();
			String userName = dbInfo.getUserName();
			String passWord = dbInfo.getPassword();
			String port = dbInfo.getPort();
			
			logger.info("OracleDbConnection: getDataBaseConnection(): Database details: host name: "+ hostname +
					    " dbName: " + dbName + " userName: "+ userName + " passWord: "+ passWord + " port: "+ port);
			
			// Database url is constructed using host name and db name.
			if (hostname != null && dbName != null && userName != null && passWord != null && port != null) {
				
				String DB_URL = "jdbc:oracle:thin:@" + hostname + ":" + port +"/" + dbName;	
				logger.info("OracleDbConnection: getDataBaseConnection(): DB URL: " + DB_URL + " oracleDriver: "+ FileReaderConstant.ORACLE_DRIVER);
				
				// Load the driver and get connection using driver manager class.
				Class.forName(FileReaderConstant.ORACLE_DRIVER);	
				connObject = DriverManager.getConnection(DB_URL, userName, passWord);
				
				logger.info("OracleDbConnection: getDataBaseConnection(): Connection Object: " + connObject);
			}
			
		} catch (SQLException e){
			logger.info("OracleDbConnection: getDataBaseConnection: SQLException: " + e.getMessage());
			
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, e.getMessage(), "");
		} catch (ClassNotFoundException e) {
			
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,FileReaderConstant.ORACLE_DRIVER+" driver not found.","");
		} 
		
		return connObject;	
	}

}
