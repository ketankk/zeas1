package com.itc.zeas.utility.connection;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.itc.zeas.exceptions.ZeasException;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.itc.zeas.utility.utility.ConfigurationReader;

/**
 * This class includes initialization of combo pooled data source and provide
 * centralized place for retrieving oozie database connection
 * 
 * @author 19217
 * 
 */
public enum OozieDataSource {

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

	/**
	 * this class holds logger instance
	 * 
	 * @author 19217
	 * 
	 */
	private static class LoggerHolder {
		private static final Logger LOGGER = Logger
				.getLogger(OozieDataSource.class);
	}

	private static ComboPooledDataSource cpds;

	private OozieDataSource()  {
		try {
			oozieInit();
		} catch (ZeasException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Function responsible for initializing of combo pooled data source
	 */
	private static void oozieInit() throws ZeasException {
		LoggerHolder.LOGGER.debug("initializing ComboPooledDataSource");
		// Properties properties = new Properties();
		// InputStream inputStream = null;
		try {
			// inputStream = new FileInputStream(System.getProperty("user.home")
			// + "/zeas/Config/config.properties");
			// properties.load(inputStream);
			String driver = ConfigurationReader.getProperty("DRIVER");
			String dbUrl = ConfigurationReader.getProperty("OOZIE_DB_URL");
			String user = ConfigurationReader.getProperty("OOZIE_DB_USER");
			String password = ConfigurationReader
					.getProperty("OOZIE_DB_PASSWORD");
			Integer minPoolSize = Integer.parseInt(ConfigurationReader
					.getProperty("OOZIE_C3PO_MIN_POOL_SIZE"));
			Integer acqireIncrementCount = Integer.parseInt(ConfigurationReader
					.getProperty("OOZIE_C3PO_ACQIRE_INCREMENT"));
			Integer maxPoolSize = Integer.parseInt(ConfigurationReader
					.getProperty("OOZIE_C3PO_MAX_POOL_SIZE"));
			Integer unreturnedConnectionTimeout = Integer
					.parseInt(ConfigurationReader
							.getProperty("OOZIE_C3PO_UNRET_CONN_TIMEOUT"));
			Boolean debugUnreturnedConnectionStackTraces = Boolean
					.parseBoolean(ConfigurationReader
							.getProperty("OOZIE_C3PO_DEBUG_UNRETURNED_CONN"));
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
			cpds.setAcquireIncrement(acqireIncrementCount);// 5
			cpds.setMaxPoolSize(maxPoolSize);// 20
		//	cpds.setUnreturnedConnectionTimeout(unreturnedConnectionTimeout);// 30
		//	cpds.setDebugUnreturnedConnectionStackTraces(debugUnreturnedConnectionStackTraces);// true
	         //required to handle Communication-link failure issue
			cpds.setTestConnectionOnCheckout(true);
			
			LoggerHolder.LOGGER
					.debug("successfully initialized ComboPooledDataSource");
		} catch (PropertyVetoException e) {
			LoggerHolder.LOGGER
					.debug("Problem while initializing ComboPooledDataSource");
			e.printStackTrace();
		}
	}
}
