package com.taphius.pipeline;

import com.itc.zeas.utility.utility.ConfigurationReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is Utility class used to generate job.properties file for Oozie workflow.
 * Properties file holds details like nameNode, jobTracker and hive-site.xml etc.
 * @author 16795
 *
 */
public class JobPropertiesFileWriter {
    
    /**
     * Method creates actual job.properties for given workflow, 
     * by loading job.properties template file.
     * @param appPath {@link String} Workflow application path
     */
    public void jobPropertiesWriter(String appPath){
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream("job.properties");
            Properties props = new Properties();
            //load job.properties file template
            props.load(is);

            props.setProperty("nameNode", ConfigurationReader.getProperty("HDFS_FQDN"));
            props.setProperty("jobTracker", ConfigurationReader.getProperty("JOB_TRACKER"));
            props.setProperty("hive_site_xml", ConfigurationReader.getProperty("HIVE_SITE_XML_PATH"));
            props.setProperty("appPath", ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA")+"/"+appPath);

            //Write these job.properties to workflow path
            FileOutputStream fOut = new FileOutputStream(new File(ConfigurationReader.getProperty("PIPELINE_APP_DATA")+"/"+appPath+"/job.properties"));
            props.store(fOut,"Workflow name : "+appPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
