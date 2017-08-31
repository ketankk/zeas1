package com.itc.zeas.profile.rdbms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import com.itc.zeas.ingestion.automatic.rdbms.DataBaseFactory;
import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseConnector;
import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseUtility;
import com.itc.zeas.project.model.NameAndDataType;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;

/*
 * This class connects to RDBMS database based on type and fetch column name, column data type and
 *  first 1000 lines of RDBMS table
 */
public class RdbmsReader implements IFileDataTypeReader {

	Logger logger = Logger.getLogger("RdbmsReader");

	private String sampleFileLoc = "";

	private String tableName;

	private Connection connObject;
	private String dbName;
	private String dbType;

	private IDataBaseUtility dbUtility;

	Map<String, String> selectedColumnDetail = new LinkedHashMap<String, String>();

	private String query;

	/**
	 * Method used for initializing the db connection and sending selected
	 * column details(name and data type)
	 * 
	 * @param filename
	 *            , dbInfo
	 * @return map
	 * @throws ZeasSQLException
	 */
	public Map<String, String> getColumnAndDataType(String filename,
			ExtendedDetails dbInfo) throws ZeasSQLException {

		Gson gSon = new GsonBuilder().create();
		Map<String, String> tablQueries = gSon.fromJson(
				dbInfo.getTableQueries(), HashMap.class);

		try {

			// Initializing stuffs needed for data base connection and query
			// execution.
			if (dbInfo != null) {
				initialize(dbInfo);

				if (tablQueries.size() > 0) {

					// Checking for query option, if so fetch query and get
					// column details
					if (tablQueries.containsKey(tableName)
							&& tablQueries.get(tableName) != null) {
						query = tablQueries.get(tableName);
						String execute_query = query + " where 1=2";

						
						if(dbType.equals(FileReaderConstant.DB2_TYPE))
							tableName=dbName+"."+tableName.toUpperCase();
						// This checks for correctness of query (syntax error,
						// table etc.)
						ResultSet rs = DbReaderUtility.executeQuery(connObject,
								execute_query);

						// Fetch column details from query.
						if (rs != null) {
							ResultSetMetaData rsmd = rs.getMetaData();
							int numOfCols = rsmd.getColumnCount();

							if (numOfCols > 0) {
								String queryTableName = rsmd.getTableName(1);
								if (queryTableName.equals(tableName)) {
									for (int i = 1; i <= numOfCols; ++i) {
										String actulaDataType = rsmd
												.getColumnTypeName(i);
										String columnName = rsmd
												.getColumnLabel(i);
										String dataType = dbUtility
												.getSupportedDataType(actulaDataType);
										selectedColumnDetail.put(columnName,
												dataType);
									}
								} else {
									throw new ZeasSQLException(
											ZeasErrorCode.SQL_EXCEPTION,
											"Please check the syntax of query",
											"");
								}
							}
						}
					}

					// If no query option, then its selected columns. Get the
					// column details form info object.
				} else {
					getColumnDetailsFormInfoObject(dbInfo);
				}
			}

			System.out
					.println("RdbmsReader.getColumnAndDataType(): selected column details: "
							+ selectedColumnDetail);

			logger.info("getColumnAndDataType: selected column details: "
					+ selectedColumnDetail);
		} catch (ZeasSQLException ex) {
			logger.info("getColumnAndDataType: SQLException: "
					+ ex.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					"Please check the syntax of query", "");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return selectedColumnDetail;
	}

	/**
	 * Method for getting the first 1000 lines from database for preview purpose
	 * 
	 * @return list
	 */
	public List<List<String>> getColumnValues() throws ZeasException {
		List<List<String>> colList = new ArrayList<>();

		try {

			// If the file exists, delete it.
			if (connObject != null) {
				File file = new File(sampleFileLoc);
				if (file.exists()) {
					file.delete();
					file = null;
				}

				// Get select query and execute query

				String selectQuery = null;
				if (query != null) {
					selectQuery = query;
				} else {
					String coulmnNameString = getColumnsString();
					//dbName="test";
					if(dbType.equals(FileReaderConstant.DB2_TYPE))
						tableName=dbName+"."+tableName.toUpperCase();
					selectQuery = dbUtility.getSelectQuery(coulmnNameString,
							tableName);
				}

				ResultSet rs = DbReaderUtility.executeQuery(connObject,
						selectQuery);
				FileWriter wr = new FileWriter(sampleFileLoc);

				ResultSetMetaData rsmd = rs.getMetaData();
				int numOfCols = rsmd.getColumnCount();
				List<String> actualDataTypelist = new ArrayList<>();

				// Fecth actual data types of selected columns.
				for (int i = 1; i <= numOfCols; ++i) {
					String actulaDataType = rsmd.getColumnTypeName(i);
					actualDataTypelist.add(actulaDataType.toUpperCase());
				}

				// Get the column values from result set and write into file
				while (rs.next()) {
					for (int i = colList.size(); i < selectedColumnDetail
							.size(); i++) {
						colList.add(new ArrayList<String>());
					}

					int j = 0;
					StringBuilder colData = new StringBuilder("");

					// Get each value from result set and put into list.
					for (int i = 1; i < (selectedColumnDetail.size() + 1); i++) {

						String actualDataType = actualDataTypelist.get(i - 1);
						String value = "";
						if (actualDataType.contains("TEXT")
								|| actualDataType.contains("CLOB")) {
							Clob clobObject = rs.getClob(i);
							if (clobObject != null) {
								long length = clobObject.length();
								if (length > 0) {
									// read first 10 character from clob data
									if (length <= 10) {
										value = clobObject.getSubString(1,
												(int) length) + " . . . . . ";
									} else {
										value = clobObject.getSubString(1, 10)
												+ " . . . . . ";
									}
								}
							} else {
								value = null;
							}
						} else {
							value = rs.getString(i);
						}
						colData.append(value);
						if (i != selectedColumnDetail.size()) {
							colData.append(",");
						}
						colList.get(j).add(value);
						j++;
					}

					// Write the content to csv file with , separation.
					wr.append(colData);
					wr.append("\n");
				}

				// Close file resources.
				wr.flush();
				wr.close();

				if (connObject != null) {
					connObject.close();
					connObject = null;
				}
				logger.info("getColumnValues colList -> Contains columns data "
						+ colList);
				logger.info("RdbmsReader: END");
			}

			// Catching required exceptions.
		} catch (SQLException ex) {
			logger.info("getColumnValues: SQLException: " + ex.getMessage());
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					ex.getMessage(), "");
		} catch (IOException ex) {
			logger.info("getColumnValues: file not found: " + ex.getMessage());
			String error = ex.getMessage();
			if (error.contains(":")) {
				error = error.substring(0, error.indexOf(":"));
			}
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,
					error, "");
		}

		return colList;
	}

	/**
	 * Method for getting connection object, dbutility object and setting file
	 * location
	 * 
	 * @param dbInfo
	 * @throws ZeasException
	 */
	private void initialize(ExtendedDetails dbInfo) throws ZeasException {

		try {

			// Get db connector and dbutility object based on data base type, by
			// connecting to connection factory.
			DataBaseFactory dbFactory = new DataBaseFactory();
			IDataBaseConnector dbConnector = dbFactory.getDbConnector(dbInfo);
			dbUtility = dbFactory.getDbUtility();
			dbType=dbInfo.getDbType();
dbName=dbInfo.getDbName();
			// Get connection object.
			connObject = dbConnector.getDataBaseConnection();
			sampleFileLoc = ConfigurationReader.getProperty("APP_DIR") + "/"
					+ dbInfo.getTableName() + FileReaderConstant.SAMPLE_FILE;
			logger.info("getDbConnection: Sample file location path: "
					+ sampleFileLoc);

			// Fetch table name from db info object.
			tableName = dbInfo.getTableName();

		} catch (ZeasSQLException ex) {
			logger.info("getDbConnection: SQLException: " + ex.toString());
			System.out.println("RdbmsReader.initialize()");
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					ex.getError(), "");
			// ex.printStackTrace();
		}
	}

	/**
	 * Method for getting columns names and construct string using columns name
	 * 
	 * @return columnsName
	 */
	private String getColumnsString() {

		String columnsName = "";
		try {
			int count = 0;
			for (String key : selectedColumnDetail.keySet()) {
				if (count != (selectedColumnDetail.size() - 1)) {
					columnsName += key + ", ";
				} else {
					columnsName += key;
				}
				++count;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.info("getColumnsString: " + columnsName);
		return columnsName;
	}

	/**
	 * Method for getting columns name and data type details from db info object
	 * sent by UI In case of query option, the list contains all column name and
	 * data type of table In case of select column option, the list contains
	 * only selected column and data type details
	 * 
	 * @param dbInfo
	 */
	private void getColumnDetailsFormInfoObject(ExtendedDetails dbInfo) {
		List<NameAndDataType> selectedColumns = new LinkedList<NameAndDataType>();

		try {
			// Get the selected column details from db info object.
			if (dbInfo.getSelectedColumnDetails() != null
					&& dbInfo.getSelectedColumnDetails().size() != 0) {
				selectedColumns = dbInfo.getSelectedColumnDetails();

				// Fetch the column name and data type details from list.
				for (int i = 0; i < selectedColumns.size(); i++) {
					NameAndDataType namDat = selectedColumns.get(i);
					String columName = namDat.getName();
					String dataType = namDat.getDataType();
					selectedColumnDetail.put(columName, dataType);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
