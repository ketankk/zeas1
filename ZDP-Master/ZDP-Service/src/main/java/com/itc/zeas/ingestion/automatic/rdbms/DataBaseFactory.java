package com.itc.zeas.ingestion.automatic.rdbms;

import com.itc.zeas.utility.filereader.*;
import com.itc.zeas.ingestion.automatic.rdbms.db2.DB2DataBaseUtility;
import com.itc.zeas.ingestion.automatic.rdbms.db2.DB2DbConnection;
import com.itc.zeas.ingestion.automatic.rdbms.mysql.MysqlDataBaseUtility;
import com.itc.zeas.ingestion.automatic.rdbms.mysql.MysqlDbConnection;
import com.itc.zeas.ingestion.automatic.rdbms.oracle.OracleDataBaseUtility;
import com.itc.zeas.ingestion.automatic.rdbms.oracle.OracleDbConnection;
import com.itc.zeas.profile.model.ExtendedDetails;
import org.apache.log4j.Logger;

/*
 * This class creates database connection and utility objectbased on data base type.
 */
/**
 * 
 * @author 20597
 * Added code for DB2 connection
 *
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
					dbConnObject = new MysqlDbConnection(dbInfo);
					dbUtility = new MysqlDataBaseUtility();
					logger.info("getDbConnector(): MYSQL_TYPE");
					break;
				case FileReaderConstant.ORACLE_TYPE:
					dbConnObject = new OracleDbConnection(dbInfo);
					dbUtility = new OracleDataBaseUtility();
					logger.info("getDbConnector(): ORACLE_TYPE");
					break;
				case FileReaderConstant.DB2_TYPE:
					dbConnObject = new DB2DbConnection(dbInfo);
					dbUtility = new DB2DataBaseUtility();
					logger.info("getDbConnector(): DB2_TYPE");
					break;
			}
		}
		return dbConnObject;
	}
	
	public IDataBaseUtility getDbUtility() {
		return dbUtility;
		
	}
}