package com.itc.zeas.v2.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.taphius.databridge.model.SchemaAttributes;
import com.itc.zeas.utility.DBUtility;

public class PigScriptTransformation extends AbstractTransformation {

	private String outputSchema;
	private String inputSchema;
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private Long executionTime;
	private String script;
	private Map<String, String> tableMapping;
	private String inputSplits;
	private String pigUdfJarPath;
	
	public PigScriptTransformation(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
		this.setTableMapping(new HashMap<String, String>());
		init();
	}
	private void init() throws Exception {
		try {
			String stageName=jsonObject.getString("stageName");
			
			super.setId(stageName);
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode= transJsonObject.readTree(entity.getJsonblob());
			this.inputSplits=rootNode.has("inputSplits")?rootNode.get("inputSplits").getTextValue():"";
			JsonNode params=rootNode.get("params");
			if(params.has("columnList")){
			JsonNode columnArr = params.path("columnList");
			Iterator<JsonNode> itr=columnArr.iterator();
			List<String> colTransList=new ArrayList<>();
			StringBuilder outSchema = new StringBuilder();
			while(itr.hasNext()){
				JsonNode  temp=itr.next();
				colTransList.add(temp.get("name").getTextValue());
				outSchema.append(temp.get("name").getTextValue()+":"+temp.get("dataType").getTextValue()+",");
			}if(outSchema.length()>0){
				this.setOutputSchema(outSchema.substring(0, outSchema.length()-1));
			}
			}else{
				this.setOutputSchema("");
			}
			/**
			 * Set udf jar path if this action has one.
			 */
			if(params.has("udfJarPath")){
				this.setPigUdfJarPath(params.get("udfJarPath").asText());
			}
			this.setScript(rootNode.get("params").get("pig").asText());
			this.parseTableMapping(rootNode.get("params").get("dataset"));
			
			String inputDataset=jsonObject.getString("input");
			Object[] arr= new HashSet<String>(Arrays.asList(inputDataset.split(","))).toArray();
			String[] inputs = Arrays.copyOf(arr, arr.length,String[].class);
			
			String tableType="entity";
			String inputLocation="";
			String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName");
			for (String input : inputs){
				int datasetId=Integer.parseInt(input.split("-")[0]);
				Entity datasetEntity=null;
				datasetEntity=DBUtility.getEntityById(datasetId);
			if(datasetEntity ==null || datasetEntity.getId()==0) {
				datasetEntity=DBUtility.getTransformationDetails(input);
				tableType="module";
			}
			String datasetJson=datasetEntity.getJsonblob();
			ObjectMapper datasetMapper = new ObjectMapper();
			JsonNode datasetRootNode = datasetMapper.readTree(datasetJson);
			
			this.setId(jsonObject.getString("stageName"));
			AbstractTransformation stage=TransformationUtil.getTransformationProperties(input);
			if( stage !=null) {
				inputLocation=stage.getOutputLocation();
				if(inputLocation.contains(",") && (!this.inputSplits.isEmpty())){
				inputLocation=this.inputSplits.equalsIgnoreCase("split1")?inputLocation.split(",")[0]:inputLocation.split(",")[1];
				this.setOutputLocation(outputLocation);
				}
			}else{
				inputLocation=datasetRootNode.get("location").asText()+"/cleansed";
				this.setOutputLocation(outputLocation);
			}
		}
			TransformationUtil.addTransformationProperties(this.getId(), this);
			this.setInputLocation(inputLocation);
			this.setOutputLocation(outputLocation);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
	
	public String getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(String outputSchema) {
		this.outputSchema = outputSchema;
	}

	public String getInputSchema() {
		return inputSchema;
	}

	public void setInputSchema(String inputSchema) {
		this.inputSchema = inputSchema;
	}

	public Map<String, String> getTableMapping() {
		return tableMapping;
	}

	public void setTableMapping(Map<String, String> tableMapping) {
		this.tableMapping = tableMapping;
	}
	private Map<String, String> parseTableMapping(JsonNode rootNode){
		Map<String, String> mapping = new HashMap<String,String>();
		Iterator<JsonNode> itr=rootNode.iterator();
		while (itr.hasNext()) {
			JsonNode jsonNode = (JsonNode) itr.next();
			/*int datasetId=Integer.parseInt(jsonNode.get("id").getTextValue().split("-")[0]);
			Entity datasetEntity=DBUtility.getEntityById(datasetId);
			if(null != datasetEntity){
				tableMapping.put(jsonNode.get("tableName").getTextValue(), datasetEntity.getName());
			}else {*/
				String split=jsonNode.has("inputSplits")?jsonNode.get("inputSplits").getTextValue():"";
				String id=jsonNode.get("id").getTextValue().replace("_","-");
				if(tableMapping.containsKey(id)){
				tableMapping.put(id,tableMapping.get(id)+"|"+jsonNode.get("tableName").getTextValue()+","+split);
				}else{
					tableMapping.put(id,jsonNode.get("tableName").getTextValue()+","+split);
				}
			//}

		}

		return mapping;

	}
	
	@Override
	public String getInputArgsString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInputDatasetSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}
	
	public static StringBuilder getSchemaForPig(List<SchemaAttributes> schemaAttributes){

        StringBuilder sm = new StringBuilder(); 
        for(SchemaAttributes schemaAttribute:schemaAttributes){
            String dataType = null;

            switch (schemaAttribute.getDataType().toLowerCase()) {
            case "varchar":
                dataType = "chararray";
                break;
            case "string":
            	dataType = "chararray";
                break;
            case "int":
                dataType = "int";
                break;
            case "long":
                dataType = "long";
                break;
            case "date":
                dataType = "chararray";
                break;
            case "time":
                dataType = "timestamp";
                break;
            case "float":
                dataType = "float";
                break;
            default:
                dataType = "chararray";
                break;
            }
            /*if(schemaAttribute.getDataType().equalsIgnoreCase("varchar")){
		  dataType = "String";
	  }else{
		  dataType = schemaAttribute.getDataType();
	  }*/

            sm.append(schemaAttribute.getName().replaceAll("[^\\p{Alpha}]+","_").trim()+":"+dataType+",");
        }
        sm.delete((sm.length()-1), sm.length());
        return sm;
    }
	
	public static StringBuilder getSchemaForPig(String schemaAttributes){

        StringBuilder sm = new StringBuilder(); 
        String schemaArray[]=schemaAttributes.split(",");
        for(int i=0;i<schemaArray.length;i++){
        	String name=schemaArray[i].split(":")[0];
        	String type=schemaArray[i].split(":")[1];
            String dataType = null;

            switch (type.toLowerCase()) {
            case "varchar":
                dataType = "chararray";
                break;
            case "string":
            	dataType = "chararray";
                break;
            case "int":
                dataType = "int";
                break;
            case "long":
                dataType = "long";
                break;
            case "date":
                dataType = "chararray";
                break;
            case "time":
                dataType = "timestamp";
                break;
            case "float":
                dataType = "float";
                break;
            default:
                dataType = "chararray";
                break;
            }
            /*if(schemaAttribute.getDataType().equalsIgnoreCase("varchar")){
		  dataType = "String";
	  }else{
		  dataType = schemaAttribute.getDataType();
	  }*/

            sm.append(name.replaceAll("[^\\p{Alpha}]+","_").trim()+":"+dataType+",");
        }
        sm.delete((sm.length()-1), sm.length());
        return sm;
    }
	public String getPigUdfJarPath() {
		return pigUdfJarPath;
	}
	public void setPigUdfJarPath(String pigUdfJarPath) {
		this.pigUdfJarPath = pigUdfJarPath;
	}


}
