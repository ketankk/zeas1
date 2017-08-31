package com.itc.taphius.utility;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

public class DBUtility {
    private static Connection connection = null;
    private static BasicDataSource mysqlDataSrc = null;
    private static Logger logger = Logger.getLogger(DBUtility.class);

    public static Connection getConnection() {
        try {
            if (connection == null || !(isConnectionValid())){
                System.out.println("Going to create new connection instance");
                Properties prop = new Properties();
                InputStream inputStream = DBUtility.class.getClassLoader()
                        .getResourceAsStream("/config.properties");
                prop.load(inputStream);
                String driver = prop.getProperty("driver");
                String url = prop.getProperty("url");
                String user = prop.getProperty("user");
                String password = prop.getProperty("password");
                Class.forName(driver);
                connection = (Connection) DriverManager.getConnection(url
                        + "?user=" + user + "&password=" + password);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * Connection validation method.
     * Checks if connection is open for longer than "wait_timeout" 
     * of MYSql DB settings. And also executes test query to validate the connection.
     * @return true if valid connection else false.
     */
    private static boolean isConnectionValid(){     
        try{
            if(connection.isClosed()){
                return false;
            }       
            (connection.prepareStatement("SELECT 1")).execute();
        }catch(SQLException ex){
            return false;
        }
        return true;
    }

    public static void getMySQLDataSource() {
        Properties props = new Properties();
        InputStream fis = null;

        try {
            fis = DBUtility.class.getClassLoader().getResourceAsStream(
                    "/config.properties");
            props.load(fis);
            mysqlDataSrc = new BasicDataSource();
            mysqlDataSrc.setUrl(props.getProperty("url"));
            mysqlDataSrc.setUsername(props.getProperty("user"));
            mysqlDataSrc.setPassword(props.getProperty("password"));
            mysqlDataSrc.setDriverClassName(props.getProperty("driver"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getSQlProperty(String param) {
        InputStream inputStream = null;
        String sqlQuery = null;

        try {
            Properties prop = new Properties();
            inputStream = DBUtility.class.getClassLoader().getResourceAsStream(
                    "/SQLEditor.properties");
            prop.load(inputStream);
            sqlQuery = prop.getProperty(param);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sqlQuery;

    }

    /**
     * This will provide database connection corresponding to oozie database
     * 
     * @return oozie database connection
     * 
     * @author 19217
     */
    public static Connection getOozieDbConnection() {
        Connection connection = null;
        String driver = null;
        try {
            Properties properties = new Properties();
            InputStream inputStream = DBUtility.class.getClassLoader()
                    .getResourceAsStream("/config.properties");
            properties.load(inputStream);
            driver = properties.getProperty("driver");
            String url = properties.getProperty("oozie_db_url");
            String user = properties.getProperty("oozie_db_user");
            String password = properties.getProperty("oozie_db_password");
            Class.forName(driver);
            connection = (Connection) DriverManager.getConnection(url
                    + "?user=" + user + "&password=" + password);
        } catch (IOException e) {
            logger.error("problem in reading content from config.properties file");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            logger.error("driver class " + driver + " not found");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("problem while creating oozie database connection");
            e.printStackTrace();
        }
        return connection;
    }
}
