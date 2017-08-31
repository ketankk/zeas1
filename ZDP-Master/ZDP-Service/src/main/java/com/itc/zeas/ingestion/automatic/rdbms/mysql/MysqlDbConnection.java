package com.itc.zeas.ingestion.automatic.rdbms.mysql;

import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.Connection;

import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseConnector;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.exceptions.ZeasException;

/*
 * This class connects to mysql database and returns the connection object.
 */
public class MysqlDbConnection implements IDataBaseConnector {

	Logger logger= Logger.getLogger("MysqlDbConnection");
	
	private ExtendedDetails dbInfo;

	public MysqlDbConnection(ExtendedDetails dbInfo) {	
		this.dbInfo = dbInfo;
	
	}

	/**
	 * Method for getting database connection
	 * @return connObject
	 */
	public Connection getDataBaseConnection()  throws ZeasException{	
		Connection connObject = null;
		
		try {
			
			String hostname = dbInfo.getHostName();
			String dbName = dbInfo.getDbName();
			String userName = dbInfo.getUserName();
			String passWord = dbInfo.getPassword();
			String port = dbInfo.getPort();
			
			logger.info("MysqlDbConnection: getDataBaseConnection(): Database details: host name: "+ hostname + " dbName: "
			            + dbName + " userName: "+ userName + " passWord: "+ passWord + " port: "+ port);
			
			if (hostname != null && dbName != null && userName != null &&  port != null) {
				
				// Database url is constructed using host name and db name.
				String DB_URL = "jdbc:mysql://" + hostname + ":" + port +"/" + dbName;	
				logger.info("MysqlDbConnection: getDataBaseConnection(): DB URL: " + DB_URL + " MysqlDriver: "+ FileReaderConstant.MYSQL_DRIVER);
				
				// Load the driver and get connection using driver manager class.
				Class.forName(FileReaderConstant.MYSQL_DRIVER);	
				connObject = DriverManager.getConnection(DB_URL, userName, passWord);
				
				logger.info("MysqlDbConnection: getDataBaseConnection(): Connection Object: " + connObject);
			}
			
		}  catch (SQLException e){
			logger.info("MysqlDbConnection: getDataBaseConnection: SQLException: " + e.toString());
			String error=e.getMessage();
			error="Unable to connect to database";
			//tempory no able to handle exact failure reason for connection
			//TODO to implement proper message for thrown exception
			/*if(error.contains(":")){
				//error= error.substring(0, error.indexOf(":"));
				error="Not able to connect database,";
			}*/
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, error, "");
		} catch (ClassNotFoundException e) {
			String error=e.getMessage();
			if(error.contains(":")){
				error= error.substring(0, error.indexOf(":"));
			}
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND, error,FileReaderConstant.MYSQL_DRIVER);
		} 
		
		return connObject;	
	}
}