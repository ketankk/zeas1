package com.itc.zeas.ingestion.automatic.bulk;

import com.itc.zeas.profile.EntityManager;
import org.apache.log4j.Logger;

public class EntityManagerController {


	EntityManagerInterface entityManagerInterface ;
	private static final Logger LOGGER = Logger.getLogger(EntityManagerController.class);
	
	public EntityManagerInterface getEntityManagerInterfaceInstance(String type){
		try {
			switch(type){
			case "Bulk":
				return new BulkEntityManager();
				default :
					return new EntityManager();
			}
		} catch (NullPointerException e) {
			LOGGER.info("NullPointer exception for type " + e.getMessage());
			return null;
		}
		
	}
	

}
