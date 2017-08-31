package com.itc.zeas.utility.connection;

import java.sql.Connection;

/*
 * This class creates database connection object based on database type.
 */
public class DBConnectionFactory {
	
	
	static Connection conn;
	public DBConnectionFactory() {
		
	}
	
	/**
	 * Method for creating data base connection object based on type
	 * @param dbType 
	 * @return dbConnObject
	 */
	public  static Connection getConnection(String dbType) {

		IDataBaseConnection dbConnObject = null;
		
		// Creating database object based on type.
		if (dbType != null) {
			switch (dbType) {

				case DBConstants.MYSQL_TYPE:
					dbConnObject = new MysqlDbConnection ();
					conn=dbConnObject.getDataBaseConnection();
					break;
			}
		}
		return conn;
	}

}
