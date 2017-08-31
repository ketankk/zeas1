package com.itc.zeas.v2.pipeline; 

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

public class HiveQueryTransformation extends AbstractTransformation {

	private String outputSchema;
	private String inputSchema;
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private Long executionTime;
	private String query;
	private Map<String, String> tableMapping;
	private String inputSplits;
	private String hiveUdfJarPath;
	private String defineFuncs;
	 

	public HiveQueryTransformation(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
		this.setTableMapping(new HashMap<String, String>());
		init();
	}

	//parse the json and assign the values for respective  info.
	private void init() {
		try {
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode= transJsonObject.readTree(entity.getJsonblob());
			this.setQuery(rootNode.get("params").get("hiveSql").asText());
			this.inputSplits=rootNode.has("inputSplits")?rootNode.get("inputSplits").getTextValue():"";
			this.parseTableMapping(rootNode.get("params").get("dataset"));
			this.setOutputSchema(rootNode.get("params"));
			//String colTransList=colAttrs.get("cloumnList").asText();			
			//String inputDataset=jsonObject.getString("input");
			String stageName=jsonObject.getString("stageName");
			super.setId(stageName);
			
			/**
			 * If defined Hive UDF set its path here.
			 */
			if(rootNode.get("params").has("udfJarPath")){
				this.setHiveUdfJarPath(rootNode.get("params").get("udfJarPath").getTextValue());
			}
			
			/**
			 * If defined Temporary functions, set it here
			 */
			if(rootNode.get("params").has("temporaryFunc")){
				this.setDefineFuncs(rootNode.get("params").get("temporaryFunc").getTextValue());
			}
			/*int datasetId=Integer.parseInt(inputDataset.split("-")[0]);
			Entity datasetEntity=null;
			datasetEntity=DBUtility.getEntityById(datasetId);
			String tableType="entity";
			if(datasetEntity ==null || datasetEntity.getId()==0) {
				datasetEntity=DBUtility.getTransformationDetails(inputDataset);
				tableType="module";
			}

			String datasetJson=datasetEntity.getJsonblob();
			ObjectMapper datasetMapper = new ObjectMapper();
			JsonNode datasetRootNode = datasetMapper.readTree(datasetJson);
			//String schemaName="";
			List<String> schemaList=new ArrayList<>();
			if(tableType.equalsIgnoreCase("module")) {
				JsonNode tempColAttrs=datasetRootNode.get("params");
				//	String colList=tempColAttrs.get("cloumnList").asText();
				//	schemaList=Arrays.asList(colList.split(","));
			}
			else{
				String schemaName=datasetRootNode.get("Schema").asText();
				schemaList=DBUtility.getColumns(schemaName);
			}*/
			String inputLocation="";
			this.setId(jsonObject.getString("stageName"));
			String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName");
			/*AbstractTransformation cacheStage=TransformationUtil.getTransformationProperties(inputDataset);
			if( cacheStage !=null) {
				inputLocation=cacheStage.getOutputLocation();
				this.setOutputLocation(outputLocation);	
			}else{
				//inputLocation=datasetRootNode.get("location").asText()+"/cleansed";
				this.setOutputLocation(outputLocation);				
			}*/
			this.setOutputLocation(outputLocation);
			TransformationUtil.addTransformationProperties(this.getId(), this);
			/*String[] colArr=colTransList.split(",");
			StringBuilder indexValues=new StringBuilder();
			int count=1;
			for(String str :colArr){
				int index=schemaList.indexOf(str.trim());
				if(count<colArr.length){
					indexValues.append(index +",");
					count++;
				}
				else{
					indexValues.append(index);
				}
			}*/

			/*count=1;
			StringBuilder schemaStr=new StringBuilder();
			for(String str :schemaList){
				if(count<schemaList.size()){
					schemaStr.append(str+",");
					count++;
				}
				else{
					schemaStr.append(str);
				}
			}*/
			this.setInputLocation(inputLocation);
			this.setOutputLocation(outputLocation);
			/*this.setOutputSchema(colTransList);
			this.setInputSchema(schemaStr.toString());
			System.out.println(indexValues);
			System.out.println(colTransList);
			System.out.println(jsonObject);
			 */
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
	
	private void setOutputSchema(JsonNode rootNode){
		JsonNode columnArr = rootNode.path("columnList");
		Iterator<JsonNode> itr=columnArr.iterator();
		StringBuilder outSchema = new StringBuilder();
		while(itr.hasNext()){
			JsonNode  temp=itr.next();
			outSchema.append(temp.get("name").getTextValue()).append(" ").append(temp.get("dataType").getTextValue()).append(",");
		}
		if(outSchema.length()>0){
			this.outputSchema = outSchema.substring(0, outSchema.length()-1);
		}
	}

	@Override
	public String getInputArgsString() {
		return null;
	}

	@Override
	public String getInputDatasetSchema() {
		// TODO Auto-generated method stub
		return null;
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

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public String getOutputDatasetSchema() {
		// TODO Auto-generated method stub
		return this.outputSchema;
	}

	public Map<String, String> getTableMapping() {
		return tableMapping;
	}

	public void setTableMapping(Map<String, String> tableMapping) {
		this.tableMapping = tableMapping;
	}

	public String getHiveUdfJarPath() {
		return hiveUdfJarPath;
	}

	public void setHiveUdfJarPath(String hiveUdfJarPath) {
		this.hiveUdfJarPath = hiveUdfJarPath;
	}

	public String getDefineFuncs() {
		return defineFuncs;
	}

	public void setDefineFuncs(String defineFuncs) {
		this.defineFuncs = defineFuncs;
	}

}
