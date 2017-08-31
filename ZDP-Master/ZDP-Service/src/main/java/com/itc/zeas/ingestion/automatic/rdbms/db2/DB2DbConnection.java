package com.itc.zeas.ingestion.automatic.rdbms.db2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseConnector;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import org.apache.log4j.Logger;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.exceptions.ZeasException;

/**
 * @author 20597 Nov 18, 2016
 */
public class DB2DbConnection implements IDataBaseConnector {

	Logger logger = Logger.getLogger("DB2DbConnection");

	private ExtendedDetails dbInfo;

	public DB2DbConnection(ExtendedDetails dbInfo) {

		this.dbInfo = dbInfo;
	}
	/*public static void main(String[] args) throws ZeasSQLException, ZeasException {
		ExtendedDetails dbInfo=new ExtendedDetails();
		dbInfo.setPort("50000");
		dbInfo.setHostName("10.6.186.5");
		dbInfo.setDbName("test");
		dbInfo.setUserName("db2inst1");
		dbInfo.setPassword("db2inst1");
		DB2DbConnection ff=new DB2DbConnection(dbInfo);
		ff.getDataBaseConnection();
	}*/
	
	/**
	 * Method for getting database connection
	 * @return connObject
	 * @throws ZeasSQLException 
	 */

	@Override
	public Connection getDataBaseConnection() throws ZeasSQLException, ZeasException {
		Connection connObject = null;

		try {
			String hostname = dbInfo.getHostName();
			String dbName = dbInfo.getDbName();
			String userName = dbInfo.getUserName();
			String passWord = dbInfo.getPassword();
			String port = dbInfo.getPort();

			logger.info("DB2DbConnection: getDataBaseConnection(): Database details: host name: " + hostname
					+ " dbName: " + dbName + " userName: " + userName + " passWord: " + passWord + " port: " + port);

			if (hostname != null && dbName != null && userName != null && port != null) {
				String DB_URL = "jdbc:db2://" + hostname + ":" + port + "/" + dbName;

				// Database url is constructed using host name and db name.
				logger.info("DB2DbConnection: getDataBaseConnection(): DB URL: " + DB_URL + " DB2Driver: "
						+ FileReaderConstant.DB2_DRIVER);
				// Load the driver and get connection using driver manager
				// class.
				Class.forName(FileReaderConstant.DB2_DRIVER);
				connObject = DriverManager.getConnection(DB_URL, userName, passWord);
				logger.info("DB2DbConnection: getDataBaseConnection(): Connection Object: " + connObject);

			}
		} catch (SQLException e) {
			logger.info("DB2DbConnection: getDataBaseConnection: SQLException: " + e.getMessage());
			String error="Unable to connect to database";

			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, error,"");
		} catch (ClassNotFoundException e) {

			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,
					FileReaderConstant.DB2_DRIVER + " driver not found.", "");
		}

		return connObject;
	}

}