package com.itc.zeas.filereader;

import org.apache.log4j.Logger;

/*
 * This class creates database connection and utility objectbased on data base type.
 */
public class DataBaseFactory {
	
	Logger logger= Logger.getLogger("DatabaseConnectionFactory");
	
	IDataBaseUtility dbUtility = null;
	
	public DataBaseFactory() {
		
	}
	
	/**
	 * Method for creating data base connection object based on type
	 * @param dbInfo 
	 * @return dbConnObject
	 */
	public IDataBaseConnector getDbConnector(ExtendedDetails dbInfo) {

		IDataBaseConnector dbConnObject = null;
		String dbType = dbInfo.getDbType();
		
		// Creating database object based on type.
		if (dbType != null) {
			switch (dbType) {

				case FileReaderConstant.MYSQL_TYPE:
					dbConnObject = new MysqlDbConnection (dbInfo);
					dbUtility = new MysqlDataBaseUtility();
					logger.info("getDbConnector(): MYSQL_TYPE");
					break;
				case FileReaderConstant.ORACLE_TYPE:
					dbConnObject = new OracleDbConnection (dbInfo);
					dbUtility = new OracleDataBaseUtility();
					logger.info("getDbConnector(): ORACLE_TYPE");
					break;
			}
		}
		return dbConnObject;
	}
	
	public IDataBaseUtility getDbUtility() {
		return dbUtility;
		
	}
}
