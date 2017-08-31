package com.itc.zeas.profile;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.itc.zeas.profile.rdbms.DbReaderUtility;
import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.rdbms.DataBaseFactory;
import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseConnector;
import com.itc.zeas.ingestion.automatic.rdbms.IDataBaseUtility;
import com.itc.zeas.project.model.NameAndDataType;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.exceptions.ZeasException;

public class RdmsMultipleTablesReader {

	Logger logger = Logger.getLogger(RdmsMultipleTablesReader.class);

	private Connection connObject;

	private IDataBaseUtility dbUtility;

	// private Connection profileDBConn;

	public static Connection getConnection(String dbType, String databaseName,
			String userName, String password, String mySQLPort, String hostUrl)
			throws Exception {
		Connection conn = null;
		if (dbType.equalsIgnoreCase("MySQL")) {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://" + hostUrl

			+ ":" + mySQLPort, userName, password);
		} else if (dbType.equalsIgnoreCase("Oracle")) {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@" + hostUrl
					+ ":" + databaseName, userName, password);
		}

		return conn;

	}

	/**
	 * Method for getting connection object, dbutility object
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

			// Get connection object.
			connObject = dbConnector.getDataBaseConnection();

		} catch (ZeasSQLException ex) {
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					ex.getError(), "");
			// ex.printStackTrace();
		}
	}

	/**
	 * Method for getting the List of Tables.
	 * 
	 * @param dbInfo
	 * @throws ZeasException
	 */
	public List<String> getTablesList(ExtendedDetails dbInfo)
			throws ZeasException {
		ResultSet rs = null;
		if (dbInfo != null)
			initialize(dbInfo);
		List<String> tableList = new ArrayList<String>();
		if (connObject != null) {
			try {
				// Create connection and execute query to get the list of tables
				String tableListQuery = dbUtility.getTablesListQuery(dbInfo
						.getDbName());
				rs = DbReaderUtility.executeQuery(connObject, tableListQuery);
				int a=rs.getFetchSize();
				//System.out.println("####################"+a);
				
				while (rs.next()) {

					switch(dbInfo.getDbType()){
					case FileReaderConstant.MYSQL_TYPE:
						tableList.add(rs.getString("table_name"));
						break;
						//check for oracle..put name of column returned
					case FileReaderConstant.ORACLE_TYPE:
						tableList.add(rs.getString("table_name"));
						break;
					case FileReaderConstant.DB2_TYPE:
						tableList.add(rs.getString("tabname"));
						break;
					}
					/*
					 * System.out.println("Table Name = " +
					 * rs.getString("table_name"));
					 */
				}
				// rs.close();
			} catch (SQLException ex) {
				logger.info("getTablesList: SQLException: " + ex.getMessage());
				throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
						ex.getMessage(), "");
				// ex.printStackTrace();
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if (connObject != null) {
					try {
						connObject.close();
					} catch (SQLException e) {
						// e.printStackTrace();
						throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
								e.getMessage(), "");
					}
				}
			}
			logger.info("getTablesList: List -> which contains List of Tables: "
					+ tableList);
		}
		return tableList;
	}

	public void CreateProfileForTheTables(ExtendedDetails dbInfo)
			throws Exception {

		Gson gSon = new GsonBuilder().create();
		// System.out.println(json);
		Map<String, List<LinkedTreeMap<String, String>>> schema = gSon
				.fromJson(dbInfo.getSelectedTableDetails(), HashMap.class);
		
		Map<String, String> tablQueries = gSon.fromJson(
				dbInfo.getTableQueries(), HashMap.class);
		Set<String> tableList = schema.keySet();
		/*
		 * System.out.println("Table List Size - " + tableList.size());
		 * System.out.println("dbInfo dbName - " + dbInfo.getDbName());
		 * System.out.println("dbInfo Name - " + dbInfo.getName());
		 * System.out.println("dbInfo.getDataSchemaType() - " +
		 * dbInfo.getDataSchemaType());
		 * System.out.println("dbInfo dbInfo.getDbType() - " +
		 * dbInfo.getDbType());
		 * System.out.println("dbInfo dbInfo.getHostName() - " +
		 * dbInfo.getHostName());
		 */

		Connection profileDBConn = null;
		PreparedStatement pstmt = null;

		// String INSERT_RECORD = "insert into entity(
		// NAME,TYPE,JSON_DATA,IS_ACTIVE,CREATED,CREATED_BY, LAST_MODIFIED
		// ,UPDATED_BY) values(?,?,?,?,?,?,?,?)";
		String sQuery = ConnectionUtility.getSQlProperty("INSERT_ENTITY");
		try {

			if (dbInfo != null) {
				initialize(dbInfo);
			}
			profileDBConn = ConnectionUtility.getConnection();
			pstmt = profileDBConn.prepareStatement(sQuery);
			int count = 0;

			String profileTableList[] = new String[tableList.size()];
			int profileCount = 0;
			 DatabaseMetaData meta = connObject.getMetaData();
			//db2 requires schema name also
				String dbSchema="";
				if(dbInfo.getDbType().equals(FileReaderConstant.DB2_TYPE))
					dbSchema=dbInfo.getDbName().toUpperCase();
				
			for (String table : tableList) {
					ResultSet resultSet=meta.getPrimaryKeys("", dbSchema, table);
					List<String> primaryKeys=new ArrayList<>();
					while (resultSet.next()) {
						primaryKeys.add(resultSet.getString(4));
						break;
					}
				count++;
				List<LinkedTreeMap<String, String>> namDatTyp = new ArrayList<LinkedTreeMap<String, String>>();
				namDatTyp = schema.get(table);
				JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
				ObjectNode parentNode = nodeFactory.objectNode();
				ObjectNode filedata = nodeFactory.objectNode();
				ArrayNode child = nodeFactory.arrayNode();// the child
				List<String> colList = new ArrayList<String>();
				String query = null;
				boolean slctAllCols = false;
				int i = 0;
				if (tablQueries.containsKey(table)
						&& tablQueries.get(table) != null) {
					query = tablQueries.get(table);
					if (query != null && !(query.isEmpty())) {
						colList = getColumnsDetailsFromQuery(query);
					}
					/*
					 * if (!colList.isEmpty() && colList.get(0).equals("ALL")) {
					 * slctAllCols = true; }
					 */
				}
				if ((!tablQueries.containsKey(table))) {
					i = 0;
					List<LinkedTreeMap<String, String>> temp = schema
							.get(table);
					for (int c = 0; c < temp.size(); c++) {
						LinkedTreeMap<String, String> map = temp.get(c);
						ObjectNode tmp = nodeFactory.objectNode();
						String name="";
						for (String objq : map.keySet()) {
							if (objq.equalsIgnoreCase("name")) {
								name=map.get(objq) ;
								tmp.put("Name",map.get(objq) );
							} else if(objq.equalsIgnoreCase("dataType")) {
								tmp.put("dataType", map.get(objq));
							}
						}
						if(primaryKeys.contains(name)){
							tmp.put("primaryKey", "YES");
						}else{
							tmp.put("primaryKey", "NO");
						}
						child.insert(i, tmp);
						i++;
					}
				}
				if (!colList.isEmpty()) {
					i = 0;
					String dataType = null;
					for (String cl : colList) {
						for (LinkedTreeMap<String, String> obj : schema
								.get(table)) {
							boolean name = false;
							for (String objq : obj.keySet()) {
								if (objq.equalsIgnoreCase("name")
										&& cl.equalsIgnoreCase(obj.get(objq))) {
									name = true;
								}
								if (objq.equalsIgnoreCase("dataType") && name) {
									dataType = obj.get(objq);
								}
							}
							if (name) {
								name = false;
								break;
							}
						}
						if (dataType == null) {
							dataType = "string";
						}
						ObjectNode tmp = nodeFactory.objectNode();
						tmp.put("Name", cl);
						tmp.put("dataType", dataType);
						if(primaryKeys.contains(cl)){
						tmp.put("primaryKey", "YES");
						}else{
							tmp.put("primaryKey", "NO");
						}
						child.insert(i, tmp);
						i++;
						dataType = null;
					}
				}

				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat(
						"MM/dd/yyyy h:mm:ss a");
				String formattedDate = sdf.format(date);
				System.out.println(formattedDate);
				String dataSchema = dbInfo.getName() + "_" + table;
				profileTableList[profileCount] = dataSchema;
				profileCount++;
				parentNode.put("name", dataSchema);
				parentNode.put("type", "DataSchema");
				parentNode.put("dataAttribute", child);
				parentNode.put("dataSchemaType", dbInfo.getDataSchemaType());
				parentNode.put("description", dataSchema);

				filedata.put("dbType", dbInfo.getDbType());
				filedata.put("port", dbInfo.getPort());
				filedata.put("rowDeli", "tab");
				filedata.put("colDeli", "tab");
				filedata.put("format", "RDBMS");
				filedata.put("tableName", table);
				filedata.put("hostName", dbInfo.getHostName());
				filedata.put("port", dbInfo.getPort());
				filedata.put("dbName", dbInfo.getDbName());
				filedata.put("userName", dbInfo.getUserName());
				if(null != dbInfo.getPassword())
					filedata.put("password", dbInfo.getPassword());
				parentNode.put("fileData", filedata);
				if (query != null) {
					parentNode.put("query", query);
				}
				System.out.println("DataYpes and Columns Json"
						+ parentNode.toString());
				pstmt.setString(1, dataSchema);
				pstmt.setString(2, "DataSchema");
				pstmt.setString(3, parentNode.toString());
				pstmt.setInt(4, 1);
				// pstmt.setTimestamp(5, new Timestamp(date.getTime()));
				pstmt.setString(5, dbInfo.getCreatedBy());
				// pstmt.setTimestamp(7, new Timestamp(date.getTime()));
				pstmt.setString(6, dbInfo.getCreatedBy());
				pstmt.addBatch();

				String dataSet = dataSchema + "_DataSet";
				ObjectNode dataSetNode = nodeFactory.objectNode();

				dataSetNode.put("dataIngestionId", dataSet);
				dataSetNode.put("name", dataSet);
				dataSetNode.put("Schema", dataSchema);
				dataSetNode.put("location", dbInfo.getDatasetlocation() + "/"
						+ dbInfo.getName() + "/" + dbInfo.getDbName() + "/"
						+ table);
				//If this dataset needs to encrypted??
				dataSetNode.put("isEncrypted", dbInfo.isEncrypted());


				pstmt.setString(1, dataSet);
				pstmt.setString(2, "DataSet");
				pstmt.setString(3, dataSetNode.toString());
				pstmt.setInt(4, 1);
				// pstmt.setTimestamp(5, new Timestamp(date.getTime()));
				pstmt.setString(5, dbInfo.getCreatedBy());
				// pstmt.setTimestamp(7, new Timestamp(date.getTime()));
				pstmt.setString(6, dbInfo.getCreatedBy());
				pstmt.addBatch();

				String dataIngestion = dataSchema + "_Schedular";
				ObjectNode dataIngestionNode = nodeFactory.objectNode();

				dataIngestionNode.put("dataIngestionId", dataIngestion);
				dataIngestionNode.put("name", dataIngestion);
				dataIngestionNode.put("dataSource", dataSchema + "_Source");
				dataIngestionNode.put("destinationDataset", dataSchema
						+ "_DataSet");
				dataIngestionNode.put("frequency", dbInfo.getFrequency());

				pstmt.setString(1, dataIngestion);
				pstmt.setString(2, "DataIngestion");
				pstmt.setString(3, dataIngestionNode.toString());
				pstmt.setInt(4, 1);
				// pstmt.setTimestamp(5, new Timestamp(date.getTime()));
				pstmt.setString(5, dbInfo.getCreatedBy());
				// pstmt.setTimestamp(7, new Timestamp(date.getTime()));
				pstmt.setString(6, dbInfo.getCreatedBy());
				pstmt.addBatch();

				String dataSource = dataSchema + "_Source";

				ObjectNode dataSourceNode = nodeFactory.objectNode();
				dataSourceNode.put("format",dbInfo.getDbType());
				dataSourceNode.put("schema", dataSchema);
				dataSourceNode.put("fileData", filedata);
				dataSourceNode.put("location", "RDBMS");
				dataSourceNode.put("dataSource", dataSource);
				dataSourceNode.put("name", dataSource);
				dataSourceNode.put("dataSourcerId", dataSource);
				dataSourceNode.put("sourcerType", "RDBMS");

				pstmt.setString(1, dataSource);
				pstmt.setString(2, "DataSource");
				pstmt.setString(3, dataSourceNode.toString());
				pstmt.setInt(4, 1);
				// pstmt.setTimestamp(5, new Timestamp(date.getTime()));
				pstmt.setString(5, dbInfo.getCreatedBy());
				// pstmt.setTimestamp(7, new Timestamp(date.getTime()));
				pstmt.setString(6, dbInfo.getCreatedBy());
				pstmt.addBatch();

				if (count == tableList.size() || count == 250) {
					int result[] = pstmt.executeBatch();
					count = 0;
					int resCount = 0;
					int tableCount = 0;
					int profIndex = 0;
					int noOfTablesToBeUpdated = 4;

					for (int res : result) {
						resCount++;
						if (res >= 0) {
							tableCount++;
						}
						if (resCount % noOfTablesToBeUpdated == 0) {
							if (tableCount == 4) {
								logger.info("User " + dbInfo.getCreatedBy()
										+ ": New ingestion profile '"
										+ profileTableList[profIndex]
										+ "' created successfully.");
							} else {
								logger.info("User " + dbInfo.getCreatedBy()
										+ ": New ingestion profile '"
										+ profileTableList[profIndex]
										+ "' creation failed.");
							}
							tableCount = 0;
							profIndex++;
						}
					}
				}
			}
			// pstmt.close();
			logger.info("-------------Profiles Created-------------");
			// profileDBConn.close();

		} catch (SQLException ex) {
			// TODO Auto-generated catch block
			throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
					ex.getMessage(), "");
		} finally {
			ConnectionUtility.releaseConnectionResources(pstmt, profileDBConn);

			if (connObject != null) {
				try {
					connObject.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				connObject = null;
			}
		}

	}

	
	private String getRequiredName(String dbType) {
		String type="Not Supported";
		switch(dbType){
		case FileReaderConstant.MYSQL_TYPE:
			return "MySQL";
		case FileReaderConstant.DB2_TYPE:
			return "DB2";
		case FileReaderConstant.ORACLE_TYPE:
			return "Oracle";
		}
		
		return type;
	}

	public Map<String, List<NameAndDataType>> getColumnDetailsForSelectedTables(
			ExtendedDetails dbInfo) throws ZeasException {

		List<String> tableList = dbInfo.getSelectedTable();
		logger.info("Table List Size - " + tableList.size());
		logger.info("dbInfo dbName - " + dbInfo.getDbName());
		Map<String, List<NameAndDataType>> result = new HashMap<String, List<NameAndDataType>>();
		if (dbInfo != null)
			initialize(dbInfo);
		if (connObject != null) {
			try {
				//Change this when you get schema name from UI
				String schemaPrefix="abcd";
				//dbInfo.getName()
				String duPprefixQuery = dbUtility.getDupPrefixCheckQuery(schemaPrefix);
				// profileDBConn = DBUtility.getConnection();
				Connection profileDBConn = ConnectionUtility.getConnection();
				
				ResultSet res = DbReaderUtility.executeQuery(profileDBConn,
						duPprefixQuery);
				if (!(res.next() && res.getLong("count") == 0)) {
					String message = "The prefix '"
							+ schemaPrefix
							+ "' has already been used please use a new prefix to create the profiles.";
					logger.info(message);
					throw new ZeasSQLException(ZeasErrorCode.ZEAS_EXCEPTION,
							message, "");
				}
				//res.close();
					DatabaseMetaData meta = connObject.getMetaData();
					//db2 requires schema name also
					String dbSchema="";
					if(dbInfo.getDbType().equals(FileReaderConstant.DB2_TYPE))
						dbSchema=dbInfo.getDbName().toUpperCase();
					
				for (String table : tableList) {
					
					ResultSet resultSet=meta.getPrimaryKeys("", dbSchema, table);
					List<String> primaryKeys=new ArrayList<>();
					
					while (resultSet.next()) {
						
						//this switch case is redundant
						switch(dbInfo.getDbType()){
						case FileReaderConstant.MYSQL_TYPE:
						primaryKeys.add(resultSet.getString(4));
						break;
						case FileReaderConstant.DB2_TYPE:

						primaryKeys.add(resultSet.getString("COLUMN_NAME"));
						break;

						}
						break;
					}
					List<NameAndDataType> columnList = new LinkedList<NameAndDataType>();
					try {
						// Create connection and execute describe query for
						// fetching column name and data type.
						String tableName=table;
						//for DB2 dbname .tablename in upper case
						switch(dbInfo.getDbType()){
						case FileReaderConstant.MYSQL_TYPE:
							 tableName=table;
						break;
						case FileReaderConstant.DB2_TYPE:

							 tableName=(dbInfo.getDbName()+"."+table).toUpperCase();
						break;

						}
						
						String describeQuery = dbUtility
								.getDescribeQuery(tableName);
						ResultSet rs = DbReaderUtility.executeQuery(connObject,
								describeQuery);
						// Get Meta data from result set.
						ResultSetMetaData rsmd = rs.getMetaData();
						int numOfCols = rsmd.getColumnCount();
						String dataType = null;
						NameAndDataType namDat = null;
						for (int i = 1; i <= numOfCols; ++i) {
							String actulaDataType = rsmd.getColumnTypeName(i);
							try {
								dataType = dbUtility
										.getSupportedDataType(actulaDataType);
							} catch (IllegalArgumentException e) {
								String[] dt = actulaDataType.split(" ");
								dataType = dbUtility
										.getSupportedDataType(dt[0]);
								e.printStackTrace();
							}
							
							System.out
									.println("RdmsMultipleTablesReader.getColumnDetailsForSelectedTables(): "+ dataType);
							if (!dataType.equals("notSupported")) {
								namDat = new NameAndDataType();
								String name=rsmd.getColumnName(i);
								namDat.setName(name);
								namDat.setDataType(dataType);
								namDat.setPrimaryKey("NO");
								if(primaryKeys.contains(name)){
								namDat.setPrimaryKey("YES");
								}
								columnList.add(namDat);
							}
						}
						result.put(table, columnList);
						rs.close();
						/*res.close();
						profileDBConn.close();
						connObject.close();*/
					} catch (SQLException ex) {
						logger.info("getColumnAndDataType: SQLException: "
								+ ex.getMessage());
						throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
								ex.getMessage(), "");
						// ex.printStackTrace();
					}
				}
			} catch (ZeasSQLException ex) {
				// TODO Auto-generated catch block
				throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
						"Unable to connect to database", "");
			} catch (SQLException ex) {
				throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION,
						ex.getMessage(), "");
			} finally {
				try {
					connObject.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		return result;
	}


	/**
	 * Method for getting column details from query
	 * 
	 * @param query
	 *            return List<String> - containing all column detail
	 */
	public List<String> getColumnsDetailsFromQuery(String query) {
		List<String> colList = new ArrayList<String>();

		try {

			if (connObject != null) {
				query += " where 1=2";
				ResultSet rs = DbReaderUtility.executeQuery(connObject, query);
				if (rs != null) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int numOfCols = rsmd.getColumnCount();
					for (int i = 1; i <= numOfCols; ++i) {
						String columnName = rsmd.getColumnName(i);
						colList.add(columnName);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return colList;
	}

	/**
	 * Method for verifying the queries
	 * 
	 * @param dbInfo
	 *            return List<String> - containing table list of failed queries
	 */
	public List<String> verifyQueries(ExtendedDetails dbInfo)
			throws ZeasException {
		List<String> faliedQueryList = new ArrayList<String>();

		try {

			if (dbInfo != null) {
				initialize(dbInfo);
				if (connObject != null) {
					Gson gSon = new GsonBuilder().create();
					Map<String, String> tablQueries = gSon.fromJson(
							dbInfo.getTableQueries(), HashMap.class);

					for (String key : tablQueries.keySet()) {
						String query = tablQueries.get(key);

						if (query != null) {
							query += " where 1=2";

							try {
								ResultSet rs = DbReaderUtility.executeQuery(
										connObject, query);
								if (rs != null) {
									ResultSetMetaData rsmd = rs.getMetaData();
									int numOfCols = rsmd.getColumnCount();
									if (numOfCols > 0) {
										String queryTableName = rsmd
												.getTableName(1);
										if (!queryTableName.equals(key)) {
											faliedQueryList.add(key);
										}
									}
								}
							} catch (ZeasSQLException e) {
								logger.info("verifyQueries: ZeasSQLException: "
										+ e.getMessage());
								faliedQueryList.add(key);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connObject != null) {
				try {
					connObject.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				connObject = null;
			}
		}

		return faliedQueryList;
	}
}