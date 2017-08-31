package com.taphius.databridge.scheduler;

import java.util.List;

import org.apache.log4j.Logger;

import com.taphius.databridge.dao.EntityDefinationDAO;
import com.taphius.databridge.model.DataSourcerAttributes;
import com.taphius.datachecker.FileChecker;

public class CustomTask {

    
	final static Logger logger = Logger.getLogger(CustomTask.class);

    EntityDefinationDAO entity;

    private FileChecker dataChecker;

    
    public FileChecker getDataChecker() {
		return dataChecker;
	}

    /**
     * @param dataChecker the dataChecker to set
     */
    public void setDataChecker(FileChecker dataChecker) {
        this.dataChecker = dataChecker;
    }


    public EntityDefinationDAO getEntityDAO() {
        return entity;
    }


    public void setEntity(EntityDefinationDAO entity) {
        this.entity = entity;
    }

    /**
     * This Job polls the DB for any newly added/updated ingestions.
     * If new ingestion is found a ***.conf properties file is created
     * and a new flume agent is started.
     * In case of existing ingestion updated, only ***.conf properties file
     * is updated and flume takes care of reloading that new editions.
     */
    public void getDBUpdates() throws Exception {

        List<DataSourcerAttributes> newEntries = entity.getNewlyAddedIngestions("DataIngestion");
        logger.info("Finished scanning for newly added Ingestions - found "+ newEntries.size());	
        
        RegisterSchedulerUtility schedulerUtility= new RegisterSchedulerUtility();
        schedulerUtility.setDataChecker(dataChecker);
        schedulerUtility.registerScheduler(newEntries);

        }

}
