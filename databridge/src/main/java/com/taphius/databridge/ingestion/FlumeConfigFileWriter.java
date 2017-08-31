
package com.taphius.databridge.ingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

import com.taphius.databridge.model.DataSourcerAttributes;
import com.taphius.databridge.runner.MainApp;

/**
 * This is Utility class takes care of writing/creating flume config file for given agent.
 * It makes use of template file to maintain the config file format 
 * and replace with per ingestion based attributes like spoolDir path, hdfs path etc.
 * @author 16795
 *
 */
public class FlumeConfigFileWriter {

    final static Logger logger = Logger.getLogger(FlumeConfigFileWriter.class);

    public static final String CONF_DIR = MainApp.FLUME_CONF_DIR;
    public static final String CONF_EXTN = ".conf";
    public static final String CustomSpoolSrc = "com.taphius.databridge.ingestion.CustomSource";
    public static final String SRC = "src-1";
    public static final String SINKS = "sink-1";
    public static final String CHANNELS = "memory-ch";
    public static final String HDFS_ROOT = ConfigurationReader.getProperty("HDFS_FQDN");

    Properties prop = new Properties();
    OutputStream output = null;
    private String agent;
    private DataSourcerAttributes dsAttr;

    /**
     * Constructor for {@link FlumeConfigFileWriter}
     * @param attr {@link DataSourcerAttributes} Ingestion details POJO
     */
    public FlumeConfigFileWriter(DataSourcerAttributes attr){
        this.dsAttr = attr;
    }


    /**
     * Method takes care writing flume config file.
     * It first checks if file already exists in /conf location,
     * if Yes, it only updates the config file,
     * else it creates new config file with attributes from DataIngestion.
     * @return {@link Boolean} <code>true</code> if new files is written,
     *                      else <code>false</code> for file updation.
     */
    public boolean writeProperties(){
        boolean fileUpdated = false;
        try {

            File conf =  new File(CONF_DIR+agent+CONF_EXTN);

            if(conf.exists()){

                //Load and read existing properties file
                FileInputStream templateFile = new FileInputStream(conf);

                prop.load(templateFile);
                templateFile.close();

                prop.setProperty(agent+".sinks.sink-1.hdfs.path", HDFS_ROOT+dsAttr.getDataSet().getLocation()+"/"+dsAttr.getName()+getHDFSTargetPath(dsAttr.getFrequency()));
                prop.setProperty(agent+".sources.src-1.spoolDir", dsAttr.getLocation());
                fileUpdated = true;
            }else {

                //Load and read template properties file
                FileInputStream templateFile = new FileInputStream(CONF_DIR+"template.conf");
                Properties templateProp = new Properties();
                templateProp.load(templateFile);
                templateFile.close();

                // Display all the values in the form of key value
                for (String key : templateProp.stringPropertyNames()) {
                    String newKey = agent+"."+key;
                    if(key.contains(".spoolDir") ){
                        logger.debug("Setting up spoolDir path - "+ dsAttr.getLocation());
                        prop.setProperty(newKey, dsAttr.getLocation());
                    }else if(key.contains(".path")){
                        String HDFSPath = HDFS_ROOT+dsAttr.getDataSet().getLocation()+"/"+dsAttr.getName()+getHDFSTargetPath(dsAttr.getFrequency());
                        logger.debug("Setting up Destination HDFS path- "+ HDFSPath);
                        prop.setProperty(newKey, HDFSPath);
                    }else {
                        prop.setProperty(newKey, templateProp.getProperty(key));
                    }
                }
            }            
            output = new FileOutputStream( conf,false);
            // save properties to conf_root folder
            prop.store(output, null);

        } catch (IOException io) {
            logger.error("Error in accesing config file - "+io.getMessage());
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    logger.error("Error closing conf file -"+e.getMessage());               
                }
            }            
        }
        return fileUpdated;
    }    

    /**
     * Helper method returns the TarhetHDFS path depending on the Frequency set for Ingestion definition.
     * @param frequency {@link String} frequency set for ingestion
     * @return {@link String} HDFS path expression 
     */
    private String getHDFSTargetPath(String frequency){
        String targetPath = "/%Y-%m-%d";
        if(frequency.equalsIgnoreCase("daily")){
            targetPath = "/%Y-%m-%d";
        }else if(frequency.equalsIgnoreCase("hourly")){
            targetPath = "/%Y-%m-%d/%H";
        }else if(frequency.equalsIgnoreCase("weekly")){
            targetPath = "/%Y-%m/%A";
        }
        return targetPath;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public DataSourcerAttributes getDsAttr() {
        return dsAttr;
    }

    public void setDsAttr(DataSourcerAttributes dsAttr) {
        this.dsAttr = dsAttr;
    }

}



