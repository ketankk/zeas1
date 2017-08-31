package com.taphius.databridge.utility;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * This class includes initialization of combo pooled data source and provide
 * centralized place for retrieving database connection
 * 
 * @author 19217
 * 
 */
public enum ZeasDataSource {

	INSTANCE {
		@Override
		public Connection getConnection() {
			Connection conn = null;
			try {
				conn = cpds.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return conn;
		}
	};

	/**
	 * Gives database connection
	 * 
	 * @return database connection
	 */
	public abstract Connection getConnection();

	private static ComboPooledDataSource cpds;

	/**
	 * this class holds logger instance
	 * 
	 * @author 19217
	 * 
	 */
	private static class LoggerHolder {
		private static final Logger LOGGER = Logger.getLogger("ZeasDataSource");
	}

	private ZeasDataSource() {
		init();
	}

	/**
	 * Function responsible for initializing of combo pooled data source
	 */
	private static void init() {
		LoggerHolder.LOGGER.debug("initializing ComboPooledDataSource");
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(System.getProperty("user.home")
					+ "/zeas/Config/config.properties");
			properties.load(inputStream);
			String driver = properties.getProperty("DRIVER");
			String dbUrl = properties.getProperty("DB_URL");
			String user = properties.getProperty("USERNAME");
			String password = properties.getProperty("PASSWD");

			Integer minPoolSize = Integer.parseInt(properties
					.getProperty("DATABRIDGE_C3PO_MIN_POOL_SIZE"));
			Integer acqireIncrementCount = Integer.parseInt(properties
					.getProperty("DATABRIDGE_C3PO_ACQIRE_INCREMENT"));
			Integer maxPoolSize = Integer.parseInt(properties
					.getProperty("DATABRIDGE_C3PO_MAX_POOL_SIZE"));
			Integer unreturnedConnectionTimeout = Integer.parseInt(properties
					.getProperty("DATABRIDGE_C3PO_UNRET_CONN_TIMEOUT"));
			Boolean debugUnreturnedConnectionStackTraces = Boolean
					.parseBoolean(properties
							.getProperty("DATABRIDGE_C3PO_DEBUG_UNRETURNED_CONN"));

			LoggerHolder.LOGGER.debug("value read from property file: "
					+ "driver: " + driver + "dbUrl: " + dbUrl + "user: " + user
					+ "password: " + password + "minPoolSize: " + minPoolSize
					+ "acqireIncrementCount: " + acqireIncrementCount
					+ "maxPoolSize: " + maxPoolSize
					+ "unreturnedConnectionTimeout: "
					+ unreturnedConnectionTimeout
					+ "debugUnreturnedConnectionStackTraces: "
					+ debugUnreturnedConnectionStackTraces);
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass(driver); // loads the jdbc driver
			cpds.setJdbcUrl(dbUrl);
			cpds.setUser(user);
			cpds.setPassword(password);

			// -- c3p0 settings --
			cpds.setMinPoolSize(minPoolSize);// 5
			cpds.setAcquireIncrement(acqireIncrementCount);// 8
			cpds.setMaxPoolSize(maxPoolSize);// 40
			cpds.setUnreturnedConnectionTimeout(unreturnedConnectionTimeout);// 30
			cpds.setDebugUnreturnedConnectionStackTraces(debugUnreturnedConnectionStackTraces);// true
			LoggerHolder.LOGGER
					.debug("successfully initialized ComboPooledDataSource");
		} catch (FileNotFoundException e) {
			LoggerHolder.LOGGER
					.debug("Problem while initializing ComboPooledDataSource");
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			LoggerHolder.LOGGER
					.debug("Problem while initializing ComboPooledDataSource");
			e.printStackTrace();
		} catch (IOException e) {
			LoggerHolder.LOGGER
					.debug("Problem while initializing ComboPooledDataSource");
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
