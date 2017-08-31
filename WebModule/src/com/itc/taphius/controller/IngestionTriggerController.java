package com.itc.taphius.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.taphius.utility.CommonUtils;
import com.itc.taphius.utility.ConfigurationReader;
import com.itc.zeas.database.SqoopImportDetails;
import com.itc.zeas.filereader.FileCopyFromLocal;
import com.itc.zeas.filereader.FileReaderConstant;
import com.itc.zeas.filereader.SampleData;
import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.dataloader.DataLoader;
import com.taphius.validation.mr.IngestionLogDetails;

@RestController
@RequestMapping("/rest/service")   
public class IngestionTriggerController {

	public static Logger LOG = Logger.getLogger(IngestionTriggerController.class);
	
	/**
	 * This method is used to trigger the Ingestion Process.
	 * Internally it calls a script which touches _DONE file at the Source dir.
	 * @param String name of Scheduler. 
	 */
	@RequestMapping(value="/runScheduler/{schedulerName}", method = RequestMethod.POST,headers="Accept=application/json")
	public  @ResponseBody void triggerScheduler(@PathVariable("schedulerName") String schedulerName){
		
		LOG.info("Got trigger ingestion request for - "+schedulerName);

		String[] strArry = null;
		CommonUtils utils =  new CommonUtils();
		SqoopImportDetails sqoop = new SqoopImportDetails();
		DataLoader dataLoader = new DataLoader();
		IngestionLogDAO ingestionLogDAO = null;
		IngestionLogDetails logDetails = null;
		String batchId = null;
		
		String sourceType = utils.getSourceType(schedulerName);
		LOG.info("Source type: "+ sourceType);
		
		switch (sourceType.toLowerCase()) {
			case "rdbms": {	
				ingestionLogDAO = new IngestionLogDAO();
				logDetails = new IngestionLogDetails();
				strArry = sqoop.getDetailsForImport(schedulerName);	
				batchId = strArry[8];
				
				logDetails.setIngestionStart("Ingestion started on |"+ new Timestamp(System.currentTimeMillis()));
				ingestionLogDAO.addLogObject(utils.getSchedularId(),batchId,"Ingestion","Started", logDetails);
			}
			break;
			case "file": {
				LOG.info("Creating _DONE file");
				strArry = new String[3];
				String SQOOP_SCRIPT_PATH = System.getProperty("user.home") + "/zeas/Config/filecreate.sh";
				String SHELL_SCRIPT_TYPE = "/bin/bash";
				strArry[0] = SHELL_SCRIPT_TYPE;
				strArry[1] = SQOOP_SCRIPT_PATH;
				strArry[2] = utils.getSourceFile(schedulerName);
			}
			break;
			default:
				LOG.error("Invalid source type.");
		}
		try {
			
			/* Pass string array as parameter to the script. First argument should be type of script, 
			 * second should be path of script and rest arguments should be user defined.
			 */
			
			// Running script using process builder
			ProcessBuilder  pb = new ProcessBuilder(strArry);
			Process p = pb.start();
			int status = p.waitFor();
			
			// Print out put of script execution on screen.
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
            while (br.ready()) {
            	LOG.info(br.readLine());
            }
            
            // Logging the errors, if any while script execution.
            BufferedReader brError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while (brError.ready()) {
				LOG.info(brError.readLine());
			}
			brError.close();
			
			// If Rdbms type, run validation rules.
			if ((sourceType.toLowerCase()).equals("rdbms")) { 
				if (status == 0) {
					logDetails.setIngestionComplete("Ingestion complete succesfully .  |"+ new Timestamp(System.currentTimeMillis()));
					ingestionLogDAO.updateLogObject(utils.getSchedularId(),batchId, "Ingestion", "Complited", logDetails);
					
					String targetPath = strArry[6];
					String[] args = new String[4];
			        args[0] = targetPath;
			        args[1] = utils.getSchemaName();
			        args[2] = utils.getFrequency();
			        args[3] = utils.getFileFormat();
			        
					String dataSet = utils.getDestDataSet();
					String ingestionId = Integer.toString(utils.getSchedularId());
					
					// Pass required information to run validation rules
					dataLoader.runValidationRules(args, dataSet, ingestionId, batchId, logDetails, ingestionLogDAO);
				} else {
					logDetails.setIngestionComplete("Ingestion failed |"+ new Timestamp(System.currentTimeMillis()));
					ingestionLogDAO.updateLogObject(utils.getSchedularId(),batchId, "Ingestion", "Failed", logDetails);
				}
			} 

		} catch (IOException e) {
			LOG.error(e.getMessage());
		} catch (InterruptedException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * This method is used to trigger the Ingestion Process.
	 * Internally it calls a script which touches _DONE file at the Source dir.
	 * @param String name of Scheduler. 
	 */
	//   @RequestMapping(value="/testRun", method = RequestMethod.POST,headers="Accept=application/json")
	@RequestMapping(value="/testRunIngestion", method = RequestMethod.POST,headers="Accept=application/json")
	public  @ResponseBody SampleData triggerTestRun(@RequestBody SampleData fileObject){
		
		System.out.println("IngestionTriggerController.triggerTestRun(): Test run ingestion");

		LOG.info("request object for test run:"+fileObject);
		SampleData sample = new SampleData();
		
		if (fileObject.getFileName() != null) {
			System.out.println("IngestionTriggerController.triggerTestRun(): "+ fileObject.getFileName());
		} else {
			System.out.println("IngestionTriggerController.triggerTestRun(): null");
		}
		if (fileObject.getFileName() != null) {
			// FileCopyFromLocal.method(file.getFileName());
			String[] strArr=fileObject.getFileName().split("/");
			String fileName=strArr[strArr.length-1];
			FileCopyFromLocal copyFromLocal= new FileCopyFromLocal();
			if(fileName.endsWith("csv") ){
				fileName=fileName.replace(".csv",FileReaderConstant.SAMPLE_FILE);
			}
			else if(fileName.endsWith("xls") ){
				fileName=fileName.replace(".xls","_"+1+ FileReaderConstant.SAMPLE_FILE);
			}
			else if(fileName.endsWith("xlsx")){
				fileName=fileName.replace(".xlsx","_"+1+ FileReaderConstant.SAMPLE_FILE);
			}
			else if(fileName.endsWith("json")){
				fileName=fileName.replace(".json",FileReaderConstant.SAMPLE_FILE);
			}
			else if(fileName.endsWith("xml")){
				fileName=fileName.replace(".xml",FileReaderConstant.SAMPLE_FILE);
			} else if (fileName.endsWith("database")) {
				fileName=fileName.replace(".database",FileReaderConstant.SAMPLE_FILE);
			}
			LOG.info("filename:"+fileObject.getFileName());
			sample = copyFromLocal.copyFromLocal(ConfigurationReader.getProperty("APP_DIR")+File.separator+fileName,
					fileObject.getTargetPath()+File.separator+"TestRun"+File.separator+fileName);
			sample.setFileName(fileName);
		}
		return sample;
	}

}
