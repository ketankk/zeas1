package com.itc.taphius.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.model.Entity;
import com.itc.taphius.model.MLAnalysis;
import com.itc.taphius.model.OozieStageStatusInfo;
import com.itc.taphius.model.PipelineStageLog;
import com.itc.taphius.model.ProcessedPipeline;
import com.itc.taphius.utility.MachineLearningExecutor;

/**
 * @author 11786
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class PipelineController {
 
    public static Logger LOG = Logger.getLogger(PipelineController.class);
           
 /**
 * This method is used to get the dynamic select query result 
 * @param String
 * @return 
 * @return List<JSONObject>
 */
	@RequestMapping(value="/pipeline/{type}/{pipelineID}", method = RequestMethod.GET,headers="Accept=application/json")

	 public  @ResponseBody String getStagesfromPipeline(@PathVariable("type") String type,@PathVariable("pipelineID") Integer pipelineID) {
		EntityManager entityManager =  new EntityManager();
		Entity entity = entityManager.getEntityById(type,pipelineID);
		String pipelineName = entity.getName();
		pipelineNotification(pipelineName);
		return "Pipeline : "+pipelineName+" ready to Start";
	
	}
	

		/*
		 * Create a notification entry for Pipeline start
		 * 
		 */
		
		public void pipelineNotification(String pipelineName) {		
			 //Load and read existing properties file
			
		    LOG.info("Going to start pipeline--"+pipelineName);
		    System.out.println("Inside START write method =================");
		    OutputStream output = null;
			try{
			
				File conf = new File(System.getProperty("user.home")+"/zeas/Config/notify");
		        FileInputStream templateFile = new FileInputStream(conf);
		        Properties prop = new Properties();
		        prop.load(templateFile);
		        templateFile.close();
		        prop.setProperty(pipelineName,"START");
		        output = new FileOutputStream( conf,false);
		        // save properties to conf_root folder
		        prop.store(output, null);
			}catch(Exception e){
				e.printStackTrace();
			}finally {
	            if (output != null) {
	                try {
	                    output.close();
	                } catch (IOException e) {
	                    LOG.error("Error saving Notify file -"+e.getMessage());               
	                }
	            }            
	        } 
	 }
		
		  
        /**
         * This method is used to get the completed processed pipelines 
         * @param String
         * @return 
         * @return List<JSONObject>
         */
            @RequestMapping(value="/pipeline/getPipelines", method = RequestMethod.GET,headers="Accept=application/json")

             public @ResponseBody List<ProcessedPipeline> getProcessedPipelines() {
                EntityManager entityMngr =  new EntityManager();
                List<ProcessedPipeline> processedPipelineList = new ArrayList<ProcessedPipeline>();
                processedPipelineList = entityMngr.getProcessedPipelines();
                return processedPipelineList;
            
            }
            
            @RequestMapping(value = "/listPipelineStageLogDetails/{entityId}", method = RequestMethod.GET, headers = "Accept=application/json")
        	public @ResponseBody
        	List<OozieStageStatusInfo> listPipelineStageLogDetails(
        			@PathVariable("entityId") Integer entityId) {
        		EntityManager entityManager = new EntityManager();
        		List<OozieStageStatusInfo> oozieLogDt = entityManager
        				.getPipelineStageLogDetailsById(entityId);
        		return oozieLogDt;
        	}
            
            
            /**
             * This method is used to get all machine Learning analysis pipelines
             * @param String
             * @return 
             * @return List<JSONObject>
             */
                @RequestMapping(value="/pipeline/getMLAnalysis", method = RequestMethod.GET,headers="Accept=application/json")

                 public @ResponseBody List<MLAnalysis> getMLAnalysis() {
                    EntityManager entityMngr =  new EntityManager();
                    List<MLAnalysis> mlAnalysisList = new ArrayList<MLAnalysis>();
                    mlAnalysisList = entityMngr.getMLAnalysis();
                    return mlAnalysisList;
                
                }

                
                /**
                 * This method is used to get the completed processed pipelines 
                 * @param String
                 * @return 
                 * @return List<JSONObject>
                 */
                    @RequestMapping(value="/pipeline/runMachineLearning", method = RequestMethod.POST,headers="Accept=application/json")

                     public void runPipelineML(@RequestBody  MLAnalysis mlAnalysis) {
                        EntityManager entityMngr =  new EntityManager();                    
                      
                         String algorithm =  mlAnalysis.getAlgorithm();
                         Entity trainingEntity= entityMngr.getEntityByName(mlAnalysis.getTraining().getDataSet());
                         Entity testingEntity = entityMngr.getEntityByName(mlAnalysis.getTesting().getDataSet());
                         JSONObject trainingObject  = new JSONObject(trainingEntity.getJsonblob());
                         JSONObject testingObject  = new JSONObject(testingEntity.getJsonblob());
                    
                         String trainingDataPath = trainingObject.getString("location");
                         String testingDataPath =  testingObject.getString("location");
                        // Call Machine Learning Excecutor 
                        MachineLearningExecutor machineLearningExecutor = new MachineLearningExecutor();
                        int accuracy = machineLearningExecutor.execute(algorithm,trainingDataPath,testingDataPath);
                         
                         // Insert Machine Learning results to DB
                         mlAnalysis.setAccuracy(accuracy);
                         entityMngr.addMLAnalysis(mlAnalysis);                      
                    
                    }
                    
                    
                    /**
                     * this method is to delete a machine Learning analysis
                    * @param entityId
                    * @return
                    */
                   @RequestMapping(value="/pipeline/deleteMLAnalysis/{mlID}", method = RequestMethod.DELETE,headers="Accept=application/json")

                    public @ResponseBody void deleteMLAnalysis(@PathVariable("mlID") Integer mlID) {
                   	 EntityManager entityManager =  new EntityManager();
                   	 entityManager.deleteEntity(mlID);	
                    } 
 
 }
