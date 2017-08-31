
package com.taphius.pipeline;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.itc.zeas.v2.pipeline.AbstractTransformation;
import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.taphius.databridge.model.SchemaAttributes;
import com.itc.zeas.utility.DBUtility;
import com.taphius.databridge.utility.ShellScriptExecutor;
//import com.taphius.databridge.utility.ShellScriptExecutor;


public class PipelineUtil {

    public static Logger LOG = Logger.getLogger(PipelineUtil.class);
    static String stageInputPath="";
    static String stageOutputPath="";

    /*
     * Watching Notification properties  Modify event call this method
     */
    public static void processPipeline(String pipelineName) throws Exception {


        Map<String, Object> pipelineStageInfo = getPipelineStages(pipelineName);
        // Pipeline info used for log pipeline process in future.
        Pipeline  pipeline = (Pipeline) pipelineStageInfo.get("pipeline");
        List<Stage>  stageList = (List<Stage>) pipelineStageInfo.get("stages");
        try{
            //Clean up stage progress details in pipeline_stage_log table
            DBUtility.cleanUpPipelineProgress(pipeline.getId());
            WorkflowBuilder wfBuilder = new WorkflowBuilder();
            LOG.info("Creating Oozie workflow for -pipeline : "+pipelineName+" start action as -"+stageList.get(0).getName());
            Document  workflowDoc = wfBuilder.getWorkFlowTemplate(pipelineName, stageList.get(0).nextAction);
            int indx =0;
            for(Stage stage:stageList){
                Element stageAction = new Element(stage.getName());
                //   DBUtility.updateStageProgress(pipeline.getId(), stage.getName(), "STARTED");
                long sPipelineTime = System.currentTimeMillis();
                DBUtility.updateStageProgress(pipeline.getId(), stage.getName(), "STARTED",new Timestamp(sPipelineTime));
                /**
                 * This is to add fork and Join nodes where-ever required.
                 */
                if(null != stage.nextAction && stage.nextAction.contains("fork-")){
                    wfBuilder.buildForkNode(workflowDoc.getRootElement(), stage);
                }

                if(stage.getJobType().equalsIgnoreCase("Hive")){
                    //                    runHiveStage(stageList, stage);   
                    stageAction = runHiveStage(stageList, stage, pipelineName); 

                }else if(stage.getJobType().equalsIgnoreCase("Pig")){
                    System.out.println("here");
                    stageAction = runPigStage(stageList, stage, pipelineName);
                }else if(stage.getJobType().equalsIgnoreCase("MapReduce")){
                    runMapReduceStage(stage);
                }else if(stage.getJobType().equalsIgnoreCase("Spark")){
                    runSparkStage(stage);

                }else if(stage.getJobType().equalsIgnoreCase("DataSet")){
                    continue;
                }
                /**
                 * This is to add fork and Join nodes where-ever required.
                 */
                if(stage.nextAction.contains("join-")){
                    wfBuilder.buildJoinNode(workflowDoc.getRootElement(), stage);
                }
                workflowDoc.getRootElement().addContent(stageAction);            

                //   DBUtility.updateStageProgress(pipeline.getId(), stage.getName(), "COMPLETED");
                long ePipelineTime = System.currentTimeMillis();
                // DBUtility.updateStageProgress(pipeline.getId(), stage.getName(), "COMPLETED ( " + (ePipelineTime-sPipelineTime)/1000+" Seconds taken)",new Timestamp(ePipelineTime));

            }
            wfBuilder.endWorkflowXML(workflowDoc.getRootElement());
            String pipelinePath = ConfigurationReader.getProperty("PIPELINE_APP_DATA")+"/"+pipelineName+"/";
            wfBuilder.saveWorkFlowXML(workflowDoc, pipelinePath+"workflow.xml");

            //Generate job.properties file 
            JobPropertiesFileWriter prop = new JobPropertiesFileWriter();
            prop.jobPropertiesWriter(pipelineName);

            //Copy workflow.xml,script and properties file to HDFS for Oozie execution            
            ShellScriptExecutor exec = new ShellScriptExecutor();
            String[] args = new String[4];
            args[0] = ShellScriptExecutor.BASH;
            args[1] = System.getProperty("user.home")+"/zeas/Config/CopyFilesFromLocalToHDFS.sh";
            args[2] = pipelinePath;
            args[3] = ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA")+"/";
            ShellScriptExecutor.runScript(args);            

            //Trigger Oozie workflow 
            runOozieWorkflow(pipelinePath+"/job.properties", pipeline.getId());
            //Add Successfully Processed Pipeline to database
            String outputDataSet = stageList.get(stageList.size()-1).getOutputDataset();
            DBUtility.saveProcessedPipeline(pipelineName,outputDataSet);
        }catch(SQLException e){
            LOG.error("An error occured while exceuting Hive Stage."+e.getMessage());
        }

    }

    /*
     * Run Hive Jobs
     */
    public static Element runHiveStage(List<Stage> stageList,Stage stage, String pipeline) throws SQLException, ZeasException {
        HiveClient hiveClient = new HiveClient();
        /*if(stage.getInputSchema()!=null){
			List<SchemaAttributes> schemaAttributeList = getSchemaAttributes(stage.getInputSchema());
			if(stage.getInputSchema().equals("FlightDataSchema")){
			    LOG.info("going to create table vi aHiveCleint--");
				hiveClient.createTable(schemaAttributeList,"Airlines",stage.getInputPath());
			}
			if(stage.getInputSchema().equals("WeatherSchema")){
				hiveClient.createTable(schemaAttributeList,"Weather",stage.getInputPath());
			}

			hiveClient.createTable(schemaAttributeList,stage.getInputSchema(),stage.getInputPath());
		}*/

        WorkflowBuilder builder = new WorkflowBuilder();
        String hqlPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA")+"/"+pipeline+"/"+stage.getName()+".hql";
        File file = new File(hqlPath);
        file.setExecutable(true);
        file.setReadable(true);
        file.setWritable(true); 
        builder.writeToFile(PipelineUtil.getHiveQuery(stage.getHiveScript(), stage.getOutputInfo().get(0).getInputPath(), stage.getiDataSet(),""), hqlPath);
        return builder.getHiveActionTemplate(stage.getName(), stage.nextAction ,ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA")+"/"+pipeline+"/"+stage.getName()+".hql");


    }


    public static List<SchemaAttributes> getSchemaAttributes(String schemaName) throws Exception {
        List<SchemaAttributes> schemaAttributes =  new ArrayList<SchemaAttributes>();
        //
        //TODO Hit DB get Schema Attributes
        //

        String json = "";
        try {
            json = DBUtility.getJSON_DATA(schemaName);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage());
        }  

        DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
        DataSchema schema  = parser.getDSConfigDetails(json);
        schemaAttributes = schema.getDataAttribute();
        LOG.info("Schema attributes =="+schemaAttributes);
        return schemaAttributes;
    }

    /*
     * Run Spark Jobs
     */

    public static void runSparkStage(Stage stage) {
        // TODO Auto-generated method stub

    }



    /*
     * Run Pig Jobs
     */

    public static Element runPigStage(List<Stage> stageList,Stage stage, String pipeline) throws SQLException, ZeasException {


        WorkflowBuilder builder = new WorkflowBuilder();
        String pigScriptPath =  ConfigurationReader.getProperty("PIPELINE_APP_DATA")+"/"+pipeline+"/"+stage.getName()+".pig";
        builder.writeToFile(stage.getPigScript(), pigScriptPath);
        return builder.getPigActionTemplate(stage.getName(), stage.nextAction, pigScriptPath,stage.getInputInfo().get(0).getInputPath(),stage.getOutputInfo().get(0).getInputPath());
        // }
    }

    /*
     * Run MapReduce Jobs
     */
    public static void runMapReduceStage(Stage stage) {
        // TODO Auto-generated method stub

    }


    /*
     * Get a list of ordered stages in a given pipeline
     * 
     */ 
    public static Map<String, Object> getPipelineStages(String pipelineName) {
        Map<String,Object> pipelineMap = new HashMap<String,Object>();
        List<Connections> connectionList = new ArrayList<Connections>();
        List<Stage> stages = new ArrayList<Stage>();
        List<Stage> finalStages = new ArrayList<Stage>();
        Map<String, Stage> finalStageMapping = new HashMap<String, Stage>();
        Pipeline pipeline = new Pipeline();
        try{
            //  
            //TODO Change to DBUtility instead of EntityManager
            //  

            Entity entity = DBUtility.getEntityDetails(pipelineName);
            if(entity != null){
                String jsonBlob = entity.getJsonblob();
                JSONObject jObject  = new JSONObject(jsonBlob); 
                LOG.info("Processing for ===="+jObject.getString("name"));
                pipeline.setId((int) entity.getId());
                pipeline.setName(jObject.getString("name"));
                pipeline.setDescription(jObject.getString("description"));
                pipeline.setFrequency(jObject.getString("frequency"));
                pipeline.setOffset(jObject.getString("offset"));
                JSONObject egObject = (JSONObject) jObject.get("ExecutionGraph");
                JSONArray srcdest = (JSONArray) egObject.get("connections");                

                JSONObject stObj = (JSONObject) jObject.get("stageList");

                JSONArray stagesList  = (JSONArray) stObj.get("stages"); 




                for(int i=0; i < stagesList.length();i++){
                    Entity each = DBUtility.getEntityDetails(stagesList.getJSONObject(i).getString("stageName"));
                    Stage eachStage = PipelineUtil.getStageDetails(each);

                    if(!finalStageMapping.containsKey(eachStage)){
                        finalStageMapping.put(eachStage.getName(), eachStage);
                        finalStages.add(eachStage);
                    }else {
                        eachStage = finalStageMapping.get(eachStage.getName());
                    }
                    String parentStr = stagesList.getJSONObject(i).getString("input");
                    String childStr = stagesList.getJSONObject(i).getString("output");

                    if(eachStage.getJobType().equalsIgnoreCase("DataSet")){
                        InputInfo out = new InputInfo();
                        List<InputInfo> outputs = new ArrayList<InputInfo>();
                        out.setInputPath(eachStage.location);
                        outputs.add(out);
                        eachStage.setOutputInfo(outputs);
                        eachStage.outputPath = eachStage.location; 
                    }

                    if(null != parentStr && !parentStr.equalsIgnoreCase("")){
                        String[] eachParent = parentStr.split(",");
                        eachStage.parent = parentStr.split(",");
                        if(eachStage.getJobType().equalsIgnoreCase("DataSet")){
                            eachStage.outputPath = eachStage.location;
                            List<InputInfo> output = new ArrayList<InputInfo>();
                            InputInfo out = new InputInfo();
                            out.setInputPath(eachStage.outputPath);
                            output.add(out);
                            eachStage.setOutputInfo(output);
                        }else{
                            for (String parent : eachParent) {

                                System.out.println(finalStageMapping.keySet());
                                Stage p = finalStageMapping.get(parent);
                                List<InputInfo> ip = eachStage.getInputInfo();
                                if(null == ip){
                                    ip = new ArrayList<InputInfo>();
                                }
                                InputInfo info = new InputInfo();
                                info.setInputPath(p.outputInfo.get(0).getInputPath());
                                ip.add(info);
                                eachStage.setInputInfo(ip);

                            }
                            List<InputInfo> output = new ArrayList<InputInfo>();
                            InputInfo out = new InputInfo();
                            out.setInputPath(eachStage.getInputInfo().get(0).getInputPath()+"/"+eachStage.getName());
                            output.add(out);
                            eachStage.setOutputInfo(output);                        

                        }



                    }

                    if(null != childStr && !childStr.equalsIgnoreCase("")){
                        String[] child = childStr.split(",");
                        eachStage.child = child;
                    }
                }

                for (Stage stage : finalStages) {
                    System.out.println(stage.getName());
                    System.out.println(stage.child);
                    System.out.println(stage.parent);
                    /*if(stage.getJobType().equalsIgnoreCase("DataSet")){
                        continue;
                    }*/
                    int childLen = (null != stage.child) ? stage.child.length : 0;
                    if(childLen == 0){
                        continue;
                    }
                    Stage c = finalStageMapping.get(stage.child[0]);
                    /**
                     * Detecting fork and join point
                     */
                    if(childLen > 1){
                        String fork = "fork";
                        for (String ch : stage.child) {
                            fork += "-"+ch;
                            Stage childStage = finalStageMapping.get(ch);
                            if(stage.getJobType().equalsIgnoreCase("DataSet") && childStage.getJobType().equalsIgnoreCase("Hive"))
                                childStage.setiDataSet(stage.getName());
                        }                        
                        stage.nextAction = fork;
                    }else if(childLen == 1){                        
                        if(c.getJobType().equalsIgnoreCase("DataSet")){
                            stage.nextAction = "end";
                            stage.setiDataSet(c.getName());

                            List<InputInfo> output = new ArrayList<InputInfo>();
                            InputInfo out = new InputInfo();
                            out.setInputPath(c.getLocation());
                            output.add(out);
                            stage.setOutputInfo(output);                        
                        }else {
                            stage.setiDataSet(stage.getName());
                            if(c.parent.length > 1){
                                String join = "join";
                                for (String st : c.parent) {
                                    join+="-"+st;
                                }
                                stage.nextAction = join;   
                            }else if(c.parent.length == 1){
                                stage.nextAction = stage.child[0];
                            }
                        }
                    }
                    System.out.println("===========");
                }
            }
        }
        /*  for(int i=0;i<srcdest.length();i++){
                    Connections conn =new Connections();
                    JSONObject stageObject = srcdest.getJSONObject(i);
                    JSONObject srcStage = (JSONObject) stageObject.get("source");
                    JSONObject destStage = (JSONObject) stageObject.get("dest");
                    int srcID =  srcStage.getInt("nodeID");
                    int destID =  destStage.getInt("nodeID");   
                    //  
                    //TODO Change to DBUtility instead of EntityManager
                    //  
                    Entity sentity = DBUtility.getEntityById(srcID);
                    Entity dsentity = DBUtility.getEntityById(destID);
                    LOG.info("Processing Src and Dest entities  ===="+dsentity);
                    conn.setSource(sentity);
                    conn.setDest(dsentity);
                    connectionList.add(conn);                   
                }           

            }           

            Map<String,List> stageMap = new HashMap<String,List>();
            Map<String,List> dataSetMap = new HashMap<String,List>();

            for(Connections con:connectionList){
                Entity src = con.getSource();
                Entity des = con.getDest();    

                if(des.getType().equals("PipelineStage")){
                    prepareMap(stageMap,src,des);          
                }else{          
                    prepareMap(dataSetMap,src,des);             
                }
            }

            // New Stage       
            Entity startStage = new Entity();

            Set entrySet=  stageMap.entrySet();
            Iterator< Entry> entryIterator = entrySet.iterator();
            while(entryIterator.hasNext()){
                Entry en = entryIterator.next();
                boolean isEntryStage = false;
                List<Entity> entityList = (List<Entity>) en.getValue();
                for(Entity sen:entityList){
                    if(!sen.getType().equals("DataSet")){
                        isEntryStage = false;
                        break;
                    }else{
                        isEntryStage = true;

                    }
                }

                if(isEntryStage){

                    Entity sentity= DBUtility.getEntityDetails(en.getKey().toString());
                    startStage = sentity;
                    Stage ss = setMultipleInputs(sentity,entityList);
                    setOutputDataset(connectionList,startStage,ss);
                    stages.add(0,ss);
                    break;
                }

            }




            int length = stageMap.size();
            List<Connections> originalList = connectionList;
            if(length>1){
                for(int i=1;i<=length-1;i++){
                    Stage nextEntry = new Stage();
                    for(Connections co:connectionList){
                        Entity sentity = co.getSource();
                        Entity dentity = co.getDest();
                        System.out.println("dentity =="+dentity.getName());
                        if(startStage.getId()==sentity.getId()){
                            startStage= dentity;   
                            nextEntry = setMultipleInputs(dentity,stageMap.get(dentity.getName()));
                            setOutputDataset(originalList,startStage,nextEntry);
                            break;
                        }
                    }

                    stages.add(i,nextEntry);
                }
            }


        }*/catch(Exception e){e.printStackTrace();}
        pipelineMap.put("pipeline", pipeline);
        pipelineMap.put("stages", finalStages);
        return pipelineMap;
    }


    /*
     * Set Output Dataset to the Last Stage
     * 
     */ 
    private static void setOutputDataset(List<Connections> originalList, Entity startStage, Stage stageEntry) {
        for(Connections co:originalList){
            Entity sentity = co.getSource();
            Entity dentity = co.getDest();
            if(dentity.getType().equalsIgnoreCase("DataSet") && sentity.getType().equalsIgnoreCase("PipelineStage")){
                if(startStage.getId() ==(sentity.getId())){
                    Stage tempStage = getOutputDetails(dentity);
                    stageEntry.setOutputPath(tempStage.getOutputPath());
                    stageEntry.setOutSchema(tempStage.getOutSchema());
                    stageEntry.setOutputDataset(tempStage.getOutputDataset());
                    break;
                }
            }

        }
    }


    /*
     * Set Stage details Json Parsing
     * 
     */ 
    public static Stage getStageDetails(Entity entity) {
        Stage stage = new Stage();
        String jsonBlob = entity.getJsonblob();
        JSONObject sObject  = new JSONObject(jsonBlob);
        try {
            if(entity.getType().equals("PipelineStage")){



                stage.setName(sObject.getString("stagename"));
                stage.setDescription(sObject.getString("stagedescription"));
                stage.setJobType(sObject.getString("seletType"));
                if(sObject.getString("seletType").equals("Pig")){
                    stage.setPigScript(sObject.getString("pig"));
                }
                if(sObject.getString("seletType").equals("Hive")){
                    stage.setHiveScript(sObject.getString("hiveSql"));
                }
                if(sObject.getString("seletType").equals("MapReduce")){
                    stage.setMapperClass(sObject.getString("mapclass"));
                    stage.setReducerClass(sObject.getString("reduceclass"));

                }

            }else{
                stage.setJobType("DataSet");
                stage.setName(sObject.getString("name")); 
                stage.setLocation(sObject.getString("location"));
            }

        } catch (JSONException e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage());
        } 
        return stage;

    }


    /*
     * Set Stage Input/Output path and schema details from dataset (Json Parser)
     * 
     */ 

    public static Stage getOutputDetails(Entity entity) {       
        Stage stage = new Stage();
        try {   
            String jsonBlob = entity.getJsonblob();             
            JSONObject sObject  = new JSONObject(jsonBlob);
            stage.setOutputPath(sObject.getString("location"));
            stage.setOutSchema(sObject.getString("Schema"));
            stage.setOutputDataset(sObject.getString("name"));

        } catch (JSONException e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage());
        } 
        return stage;

    }

    public static InputInfo getInputDetails(Entity entity) {
        InputInfo inputInfo = new InputInfo();
        try {   
            String jsonBlob = entity.getJsonblob();             
            JSONObject sObject  = new JSONObject(jsonBlob);
            inputInfo.setInputPath(sObject.getString("location"));
            inputInfo.setInputSchema(sObject.getString("Schema"));
            inputInfo.setInputDataSet(sObject.getString("name"));

        } catch (JSONException e) {
            LOG.error(e.getMessage());
        }
        return inputInfo;

    }

    private static void prepareMap(Map<String, List> emap, Entity src,Entity des) {            // TODO Auto-generated method stub
        if(emap.containsKey(des.getName())){
            List<Entity> output =emap.get(des.getName());
            output.add(src);
            emap.put(des.getName(),output);
        }else{
            List<Entity> output = new ArrayList<Entity>();
            output.add(src);
            emap.put(des.getName(),output);
        }
    }

    private static Stage setMultipleInputs( Entity sentity,List<Entity> entityList){
        Stage ss = getStageDetails(sentity);
        List<InputInfo> tempList = new ArrayList<InputInfo>();
        for(Entity ssen:entityList){                
            // Todo add array of input path, input schema, input data set
            if(ssen.getType().equals("DataSet")){
                InputInfo inputInfo =  getInputDetails(ssen);
                tempList.add(inputInfo);
            }  
        }
        if(tempList.size()>0){
            ss.setInputInfo(tempList);
        }
        return ss;
    }

    public static void runOozieWorkflow(String propFilePath, int pipelineId) throws Exception {
        /*ShellScriptExecutor exec = new ShellScriptExecutor();
        String[] args = new String[5];
        args[0] = ShellScriptExecutor.BASH;
        args[1] = System.getProperty("user.home")+"/zeas/Config/triggerOozieWorkflow.sh";
        args[2] = "-oozie "+ConfigurationReader.getProperty("OOZIE_ENGINE");
        args[3] = "-config "+propFilePath;
        args[4] = "-run";
        exec.runScript(args);  */ 
    	 String line = "";
         String oozieJobId = "";
         BufferedReader br = null;
        try{
        String[] command = { ShellScriptExecutor.BASH, System.getProperty("user.home")+"/zeas/Config/triggerOozieWorkflow.sh","-oozie "+ConfigurationReader.getProperty("OOZIE_ENGINE"), "-config "+propFilePath, "-run"};
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process oozieProcess = processBuilder.start();
         br = new BufferedReader(new InputStreamReader(
                      oozieProcess.getInputStream()));
        while ((line = br.readLine()) != null) {
               if (line.startsWith("job: ")) {
                      oozieJobId = line.substring(5);
                      System.out.println("oozieJobId: " + oozieJobId);
                      DBUtility.addJobMappingInDb(pipelineId, oozieJobId);
                      break;
               }
        }
        
        // logs the error
        br = new BufferedReader(new InputStreamReader(
                      oozieProcess.getErrorStream()));
        while ((line = br.readLine()) != null) {
               System.out.println("SCRIPT_ERROR:" + line);
        }
        br.close();
        }catch(IOException | SQLException ex){
        	LOG.error("SQLException occured while executing sql select query for inserting oozie job id and pipeline id "+oozieJobId);
        	ex.printStackTrace();
            LOG.error("Error invoking Oozie JOB for pipeline - "+pipelineId);
        }
        finally{
        	//closing bufferedReader 
            if(br != null){
            	try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
    }
//Hive code for lineage
    public static String getHiveQuery(String hiveScript, String outputPath, String dataset,String additionalQuery) {
        StringBuilder query = new StringBuilder("hive -e \"use zeas;");
        query.append("drop table if exists "+dataset+";");
        System.out.println("hiveScript===" + hiveScript);
        query.append("create table if not exists "+dataset+" row format delimited fields terminated by',' as "+ hiveScript +";\"");
        LOG.info("Hive is executing query here ======="+query);
        return query.toString();
    }
    
/*    public static String getHiveQuery(String hiveScript, String outputPath, String dataset,String additionalQuery) {
        StringBuilder query = new StringBuilder("use zeas; \n");
        query.append(additionalQuery);
        query.append("drop table if exists "+dataset+"; \n");
       // if(additionalQuery!= null && additionalQuery.equalsIgnoreCase(""))
        query.append("dfs -mkdir -p "+outputPath+"; \n");
        query.append("create table if not exists "+dataset+" row format delimited fields terminated by',' location '"+outputPath+"' as "+ hiveScript +"; \n");
        LOG.info("Hive is executing query here ======="+query);
        return query.toString();
    }*/
    
    public static String getPigScript(String pigScript, String outputPath, String dataset,Map<String, AbstractTransformation> inputTablesToLoad, String pigUDFPath) {
    	StringBuilder query=new StringBuilder();
    	if(null != pigUDFPath && !pigUDFPath.isEmpty()){
    		query.append(pigUDFPath);
    	}
    	String name = null;
    	for(Map.Entry<String, AbstractTransformation> entry:inputTablesToLoad.entrySet()){
    		name=entry.getKey();
         query.append(name+" = load '"+entry.getValue().getOutputLocation()+"' USING PigStorage(',') as ("+entry.getValue().getDatasetSchema()+"); \n");
    	}
    	pigScript=URLDecoder.decode(pigScript);
        String outPut=pigScript.contains("=")?pigScript.substring(pigScript.lastIndexOf('\n')+1,pigScript.indexOf("=", pigScript.lastIndexOf('\n'))).trim():"";
        if(pigScript.contains("Table")){
        pigScript=	pigScript.replaceAll("Table", "table");
        }
        if(pigScript.contains("=")){
            pigScript=	pigScript.replaceAll("=", " = ");
        }
        query.append(pigScript+"\n");
        query.append("store "+name+" into '"+outputPath+"' USING PigStorage(',');\n");
        return query.toString();
    }
    
    public static int copyJarFile(String pipelineName,String jarName){

        if(jarName!=null && pipelineName!=null){
            File sourceLocation=new File(jarName);
            String jarFileName = jarName.substring(jarName.lastIndexOf("/")+1);
            File targetLocation=new File(pipelineName+"/lib/");
            if (targetLocation.exists()){
            	targetLocation.delete();
            }
            if(!targetLocation.exists()){
                targetLocation.mkdirs();
            }
            targetLocation=new File(pipelineName+"/lib/"+jarFileName);
            try {
                InputStream in = new FileInputStream(sourceLocation);
                OutputStream out = new FileOutputStream(targetLocation);

                // Copy the bits from instream to outstream
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                in.close();
                out.close();
                LOG.info("Jar file copied sucessfully from "+sourceLocation+" to "+targetLocation);
                return 1;
            } catch (IOException e) {
                LOG.error("Copy process of jar file failed due to "+e.toString());
                return 0;
            }
        }
        LOG.error("Jar or pipeline doesn't exist");
        return -1;
    }



}
