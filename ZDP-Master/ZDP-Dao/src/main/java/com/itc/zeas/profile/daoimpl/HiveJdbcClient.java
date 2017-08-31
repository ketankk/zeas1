package com.itc.zeas.profile.daoimpl;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 20020 This class perform the Select related operation on Hive through
 *         JDBC
 */
public class HiveJdbcClient {
    final private static Logger LOGGER = Logger.getLogger(HiveJdbcClient.class);

    final private static String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static int HIVE_LIMIT;
    private String HIVE_JDBC_HOST;
    private String HIVE_JDBC_USER;
    private String HIVE_JDBC_PASS;
    private String HIVE_JDBC_DB;

    /**
     * @return list of hive record
     */
    public HiveJdbcClient() throws ZeasException {

        HIVE_LIMIT = Integer.parseInt(ConfigurationReader.getProperty("HIVE_TABLE_LIMIT"));
        HIVE_JDBC_HOST = ConfigurationReader.getProperty("HIVE_JDBC_HOST");
        HIVE_JDBC_DB = ConfigurationReader.getProperty("HIVE_JDBC_DB");
        HIVE_JDBC_USER = ConfigurationReader.getProperty("HIVE_JDBC_USER");
        HIVE_JDBC_PASS = ConfigurationReader.getProperty("HIVE_JDBC_PASS");

    }

    public List<Map<String, String>> getResultByExecuteHiveQuery(String datasetHiveQuery) throws ZeasException {
        ResultSet resultSet = null;
        ResultSetMetaData metaData;
        Statement statement = null;
        int columnCount;
        Connection connection = getHiveConnection();
        List<Map<String, String>> listofHiveRecordMap = new ArrayList<Map<String, String>>();

        try {
            statement = connection.createStatement();
            try {
                resultSet = statement.executeQuery(datasetHiveQuery);
            } catch (SQLException e) {
                throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, "Error in Query ", "");
            }
            while (resultSet.next()) {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();

                Map<String, String> map = new HashMap<>();
                ;
                for (int i = 1; i < columnCount + 1; i++) {

                    String colName = metaData.getColumnLabel(i);
                    //for select * it returns column name as tableName.columnName

                    if (colName.contains(".")) {
                        colName = colName.split("\\.")[1];
                    }
                    String value = resultSet.getString(colName);
                    map.put(colName, value);

                }
                listofHiveRecordMap.add(map);


                // get only limited rows
                if (listofHiveRecordMap.size() == HIVE_LIMIT)
                    break;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            /* Closing all the neccesary connection. */
            ConnectionUtility.releaseConnectionResources(resultSet, statement, connection);

        }

        return listofHiveRecordMap;
    }

    /**
     * Below connection used to instantiate the hive connection.
     *
     * @return Hive connection class
     */
    private Connection getHiveConnection() throws ZeasException {
        try {
            /* load the driver and return the connection */
            Class.forName(HIVE_DRIVER_NAME);
            String connUrl = "jdbc:hive2://" + HIVE_JDBC_HOST + "/" + HIVE_JDBC_DB;
            return DriverManager.getConnection(connUrl, HIVE_JDBC_USER, HIVE_JDBC_PASS);

        } catch (ClassNotFoundException | SQLException e) {
            LOGGER.error("Exception caught in HiveJDBCCleint: " + e.getLocalizedMessage());
            throw new ZeasException(ZeasErrorCode.CONNECTION_EXCEPTION, "Couldn't create hive connection " + e.getMessage());
        }

    }

}