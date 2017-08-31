package com.itc.zeas.utility.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

/*
 * This class connects to mysql database and returns the connection object.
 */
public class MysqlDbConnection implements IDataBaseConnection {

	static Logger LOG=Logger.getLogger(MysqlDbConnection.class);
	private static Connection connection = null;

	public MysqlDbConnection() {

	}

	/**
	 * Method for getting database connection
	 * 
	 * @return connObject
	 */
	public Connection getDataBaseConnection() {
		try {

			if (connection == null || !(isConnectionValid())) {
				LOG.info("getDataBaseConnection: MysqlDbConnection: Going to create new connection instance");
				LOG.info("getDataBaseConnection: MysqlDbConnection: : load input stream");

				String driver = ConfigurationReader.getProperty("DRIVER");
				String url = ConfigurationReader.getProperty("DB_URL");
				String user = ConfigurationReader.getProperty("USERNAME");
				String password = ConfigurationReader.getProperty("PASSWD");

				LOG.info("dbconnection.getConnection(): " + driver
						+ "  " + url + "  " + user + "  " + password);
				Class.forName(driver);
				connection =  DriverManager.getConnection(url
						+ "?user=" + user + "&password=" + password);
			}
		} catch (ClassNotFoundException |SQLException  e) {
			e.printStackTrace();
			LOG.error("Error ocuured "+e.getMessage());

		} catch (Exception e) {
			LOG.error("Error ocuured "+e.getMessage());
		}

		return connection;
	}

	private static boolean isConnectionValid() {
		try {
			if (connection.isClosed()) {
				LOG.info("Connection is not valid");
					return false;
			}
			(connection.prepareStatement("SELECT 1")).execute();
		} catch (SQLException ex) {
			LOG.info("Connection is not valid");
			return false;
		}
		LOG.info("Connection is valid");
		return true;
	}
}
