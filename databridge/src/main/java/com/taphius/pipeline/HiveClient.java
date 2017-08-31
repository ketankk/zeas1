package com.taphius.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.taphius.databridge.model.SchemaAttributes;
import com.taphius.databridge.utility.ShellScriptExecutor;

public class HiveClient {
	final static Logger logger = Logger.getLogger(HiveClient.class);
	private static Connection connection = null;

	/*
	 * Create Hive Connection
	 */
	public static Connection getHiveConnection() throws SQLException {
		if (connection != null) {
			logger.info("Returning existing connection");
			return connection;
		} else {
			try {
				Properties prop = new Properties();
				InputStream inputStream = HiveClient.class.getClassLoader()
						.getResourceAsStream("hiveconnection.properties");
				prop.load(inputStream);
				String driver = prop.getProperty("driverName");
				String url = prop.getProperty("url");
				String userName = prop.getProperty("user");
				String password = prop.getProperty("password");
				String database = prop.getProperty("database");
				logger.info("Hive establishing connection ==" + connection);
				Class.forName(driver);
				connection = DriverManager.getConnection(url, userName, password);
				logger.info("Hive Connection details ==" + connection);
				System.out.println("After");
				// Use Database;
				Statement stmt = connection.createStatement();
				String sql = "create database if not exists " + database;
				logger.info("Using Hive schema ===" + sql);
				stmt.executeUpdate(sql);

				sql = "use " + database;
				logger.info("Using Hive schema ===" + sql);
				stmt.executeUpdate(sql);

				/*
				 * sql = "add jar /user/16795/CustomUDF.jar" ;
				 * stmt.executeUpdate(sql);
				 * 
				 * sql =
				 * "create temporary function get_holiday AS 'com.hiveudf.holiday.NearestHoliday'"
				 * ; stmt.executeUpdate(sql);
				 * 
				 * sql =
				 * "create temporary function get_hour AS 'com.hiveudf.holiday.GetHour'"
				 * ; stmt.executeUpdate(sql);
				 * 
				 * sql =
				 * "create temporary function get_date AS 'com.hiveudf.holiday.GetDate'"
				 * ; stmt.executeUpdate(sql);
				 */
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return connection;
		}
	}

	/*
	 * Create Table Schema
	 */
	public void createTable(List<SchemaAttributes> schemaAttributes, String tableName, String inputPath)
			throws SQLException {
		Connection con = getHiveConnection();
		Statement stmt = con.createStatement();
		StringBuilder sm = getSchemaAttributes(schemaAttributes);
		String createQuery = "create external table if not exists " + tableName + "(" + sm.toString() + ")"
				+ " row format delimited fields terminated by',' lines terminated by'\n' location '" + inputPath + "'";
		logger.info("Creating table -------------------" + createQuery);
		stmt.executeUpdate(createQuery);
	}

	/**
	 * Method loads the data into existing table. Its appends the data to
	 * existing table(No-Overwrite)
	 * 
	 * @param tableName
	 *            {@link String} name of the table to load data
	 * @param dataPath
	 *            {@link String} Path from where to load data.
	 * @throws SQLException
	 */
	public void loadDataIntoTable(String tableName, String dataPath) throws SQLException {
		Connection con = getHiveConnection();
		Statement stmt = con.createStatement();
		String loadQuery = "LOAD  DATA  INPATH  '" + dataPath + "' INTO TABLE " + tableName;
		stmt.executeQuery(loadQuery);
	}

	/*
	 * Getting Schema List
	 */
	public static StringBuilder getSchemaAttributes(List<SchemaAttributes> schemaAttributes) {

		StringBuilder sm = new StringBuilder();
		for (SchemaAttributes schemaAttribute : schemaAttributes) {
			String dataType = null;
			dataType=getDataType(schemaAttribute.getDataType());
			sm.append(schemaAttribute.getName() + " " + dataType + ",");
		}
		sm.delete((sm.length() - 1), sm.length());
		logger.info("Table schema is =" + sm.toString());
		return sm;
	}

	public static void registerDataset(String schemaStr, String tableName, String dataPath, String batchId) {

		DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
		DataSchema schema = parser.getDSConfigDetails(schemaStr);

		if (dataPath.charAt(dataPath.length() - 1) != '/') {
			dataPath = dataPath + "/";
		}

		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home") + "/zeas/Config/createHiveTable.sh";
		args[2] = tableName;
		args[3] = HiveClient.getSchemaAttributes(schema.getDataAttribute()).toString() + ",ingestionTime timestamp"
				+ ",sourceFile string";
		args[4] = dataPath + "cleansed";
		args[5] = dataPath;
		args[6] = dataPath + batchId;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);
		registerView(schemaStr, tableName);
		/*
		 * try { hclient.createTable(schema.getDataAttribute(), tableName,
		 * dataPath); hclient.loadDataIntoTable(tableName, dataPath); } catch
		 * (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
	}

	private static void registerView(String schemaStr, String tableName) {

		DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
		DataSchema schema = parser.getDSConfigDetails(schemaStr);

		String[] args = new String[6];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home") + "/zeas/Config/createHiveView.sh";
		args[2] = schema.getName() + "_view";
		args[3] = HiveClient.getColumnList(schema.getDataAttribute());
		args[4] = HiveClient.getCompositeKey(schema.getDataAttribute());
		args[5] = tableName;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);
	}

	private void registerQuarantine(String tableName, String dataPath, String batchId) {

		if (dataPath.charAt(dataPath.length() - 1) != '/') {
			dataPath = dataPath + "/";
		}
		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home") + "/zeas/Config/createHiveTable.sh";
		args[2] = tableName;
		args[3] = HiveClient.getSchemaAttributes(schemaForQuarantine()).toString();
		args[4] = dataPath + "quarantine";
		args[5] = dataPath;
		args[6] = dataPath + batchId;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);

		// HiveClient hclient = new HiveClient();
		// try {
		// hclient.createTable(schemaForQuarantine(), tableName, dataPath);
		// hclient.loadDataIntoTable(tableName, dataPath);
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public static String getColumnList(List<SchemaAttributes> dataAttribute) {
		StringBuilder columnList = new StringBuilder();
		for (SchemaAttributes attributes : dataAttribute) {
			if (attributes.getName() != null) {
				columnList.append(attributes.getName() + ",");
			}
		}
		if (columnList.length() > 0) {
			columnList.delete((columnList.length() - 1), columnList.length());
		}
		return columnList.toString();
	}

	public static String getCompositeKey(List<SchemaAttributes> dataAttribute) {
		StringBuilder compositeKey = new StringBuilder();
		for (SchemaAttributes attributes : dataAttribute) {
			if (attributes.getPrimary() != null) {
				if (attributes.getPrimary().equalsIgnoreCase("yes")) {
					compositeKey.append(attributes.getName() + ",");
				}
			}
		}
		if (compositeKey.length() > 0) {
			compositeKey.delete((compositeKey.length() - 1), compositeKey.length());
		}
		return compositeKey.toString();
	}

	/*
	 * Run Hive Script
	 */
	public String runHiveScript(String hiveScript, String outputPath, String dataset) throws SQLException {
		// Connection con = getHiveConnection();
		// Statement stmt = con.createStatement();
		String cleanUp = "use zeas; \n drop table if exists " + dataset + "; \n";
		// stmt.executeQuery(cleanUp);
		// String query = "Insert overwrite directory '" + outputPath+"' "+
		// hiveScript;
		String query = "create table if not exists " + dataset
				+ " row format delimited fields terminated by',' location '" + outputPath + "' as " + hiveScript
				+ "; \n";
		// location '" + outputPath+"' AS "+ hiveScript;
		logger.info("Hive is executing query here =======" + query);
		// stmt.executeQuery(query);

		logger.info("Completed execution of Hive Stage..");
		return cleanUp + query;

		//
		// TODO store output to outputpath/outputschema.
		//
	}

	public static String getRegisterTblQuery(String tblName, String schema, String location) {
		schema = getSchemaForHive(schema);
		return "drop table if exists " + tblName + ";\n create external table if not exists " + tblName + "(" + schema
				+ ")" + " row format delimited fields terminated by',' lines terminated by'\n' location '" + location
				+ "';\n";
	}

	/**
	 * Make schema to support hive data type.
	 * 
	 * @param schema
	 * @return hive supported schema.
	 */
	private static String getSchemaForHive(String schema) {
		String schemaList[] = schema.split(",");
		StringBuilder schemaBuilder = new StringBuilder();
		for (String s : schemaList) {
			String schemaKeyVal[] = s.split(":");
			schemaBuilder.append(schemaKeyVal[0]).append(" " + getDataType(schemaKeyVal[1])).append(",");
		}
		if (schemaBuilder.length() > 0) {
			schema = schemaBuilder.substring(0, schemaBuilder.length() - 1).toString();
		}
		return schema;
	}

	/**
	 * Return the hive supported data type by taking parameter sql data type. 
	 * @param sql data type.
	 * @return hive data type.
	 */
	private static String getDataType(String value) {
		String dataType = "string";
		switch (value.toLowerCase().trim()) {
		case "varchar":
			dataType = "String";
			break;
		case "int":
			dataType = "int";
			break;
		case "long":
			dataType = "bigint";
			break;
		case "date":
			dataType = "date";
			break;
		case "time":
			dataType = "timestamp";
			break;
		case "float":
			dataType = "float";
			break;
		default:
			dataType = "String";
			break;
		}
		return dataType;
	}

	private List<SchemaAttributes> schemaForQuarantine() {

		List<SchemaAttributes> attrs = new ArrayList<SchemaAttributes>();
		SchemaAttributes rule = new SchemaAttributes("Rule", "varchar");
		SchemaAttributes expected = new SchemaAttributes("Expected", "varchar");
		SchemaAttributes found = new SchemaAttributes("Found", "varchar");
		SchemaAttributes column = new SchemaAttributes("Column_Name", "varchar");
		SchemaAttributes time = new SchemaAttributes("errTime", "varchar");
		SchemaAttributes record = new SchemaAttributes("Record", "varchar");
		SchemaAttributes ingestionTime = new SchemaAttributes("IngestionTime", "Timestamp");
		SchemaAttributes fileName = new SchemaAttributes("sourceFile", "varchar");
		attrs.add(rule);
		attrs.add(expected);
		attrs.add(found);
		attrs.add(column);
		attrs.add(time);
		attrs.add(ingestionTime);
		attrs.add(fileName);
		attrs.add(record);
		return attrs;
	}

}
