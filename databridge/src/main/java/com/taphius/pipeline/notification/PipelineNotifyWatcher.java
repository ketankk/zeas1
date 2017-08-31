package com.taphius.pipeline.notification;

import com.taphius.datachecker.FileChecker;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;



public class PipelineNotifyWatcher {

    final static Logger logger = Logger.getLogger(PipelineNotifyWatcher.class);

private FileChecker dataChecker;

    
    /**
     * @param dataChecker the dataChecker to set
     */
    public void setDataChecker(FileChecker dataChecker) {
        this.dataChecker = dataChecker;
    }



   

    public void init(){
        try{            
            //register path
            Path toWatch = Paths.get(ConfigurationReader.getProperty("NOTIFY_FILE_PATH"));
            toWatch.register(dataChecker.getMyWatcher(), ENTRY_MODIFY);
            //th.join();
            logger.info("Registered for file at path - "+ConfigurationReader.getProperty("NOTIFY_FILE_PATH"));
        }catch(Exception e){
            logger.error("There is an error iniating File checker - "+e.getMessage());
        }
    }


}
