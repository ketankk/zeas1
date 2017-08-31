package com.itc.zeas.v2.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.itc.zeas.profile.model.Entity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.itc.zeas.utility.DBUtility;

public class ColumnMissingDataTransformation extends AbstractTransformation {

	
	private String outputSchema;
	private String inputSchema;
	private String transColumnIndex; //transformation column index comma sepearted
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private String operator;
	private String value;
	private List<String> schmeAndDatatypeList;
	private Long executionTime;
	private String inputSplits;

	public ColumnMissingDataTransformation(Entity entity,JSONObject jsonObject,String projectName,Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
		schmeAndDatatypeList=new ArrayList<>();
		init();
	}

	//parse the json and assign the values for respective  info.
	private void init() throws Exception {
		try {
		ObjectMapper transJsonObject = new ObjectMapper();
		JsonNode rootNode= transJsonObject.readTree(entity.getJsonblob());
		JsonNode colAttrs=rootNode.get("params");
		JsonNode columnArr = colAttrs.path("selectedcolumnList");
		this.inputSplits=rootNode.has("inputSplits")?rootNode.get("inputSplits").getTextValue():"";
		Iterator<JsonNode> itr=columnArr.iterator();
		List<String> colTransList=new ArrayList<>();
		while(itr.hasNext()){
			JsonNode  temp=itr.next();
			colTransList.add(temp.has("name")?temp.get("name").getTextValue():"");
		}
		this.setOperator(colAttrs.get("oparater").asText());
		if(this.getOperator().equalsIgnoreCase("Custom Value"))
			this.setValue(colAttrs.get("value").asText());
		String inputDataset=jsonObject.getString("input");
		String stageName=jsonObject.getString("stageName");
		super.setId(stageName);
		int datasetId=Integer.parseInt(inputDataset.split("-")[0]);
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
			JsonNode tempRootNode= transJsonObject.readTree(datasetEntity.getJsonblob());
			JsonNode tempColAttrs=tempRootNode.get("params");
			JsonNode colList=tempColAttrs.get("columnList");
			Iterator<JsonNode> colListItr=colList.iterator();
			while(colListItr.hasNext()){
				JsonNode  temp=colListItr.next();
				schemaList.add(temp.get("name").getTextValue());
				schmeAndDatatypeList.add(temp.get("name").getTextValue()+":"+temp.get("dataType").getTextValue());
			}
			
		}
		else{
			String schemaName=datasetRootNode.get("Schema").asText();
			schemaList=DBUtility.getColumns(schemaName);
			schmeAndDatatypeList=DBUtility.getColumnAndDatatype(schemaName);
		}
		String inputLocation="";
		this.setId(jsonObject.getString("stageName"));
		//Date date= new  Date();
		String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName");
		AbstractTransformation cacheStage=TransformationUtil.getTransformationProperties(inputDataset);
		if( cacheStage !=null) {
			inputLocation=cacheStage.getOutputLocation();
			if(inputLocation.contains(",") && (!this.inputSplits.isEmpty())){
			inputLocation=this.inputSplits.equalsIgnoreCase("split1")?inputLocation.split(",")[0]:inputLocation.split(",")[1];
			this.setOutputLocation(outputLocation);
			}
		}else{
			inputLocation=datasetRootNode.get("location").asText()+"/cleansed";
			this.setOutputLocation(outputLocation);			
		}
		TransformationUtil.addTransformationProperties(this.getId(), this);
		//String[] colArr=colTransList.split(",");
		StringBuilder indexValues=new StringBuilder();
		int count=1;
		for(String str :colTransList){
			int index=schemaList.indexOf(str.trim());
			indexValues.append(index);
			if(count<colTransList.size()){
				indexValues.append(",");
			}
			count++;
		}
		
		count=1;
		StringBuilder schemaStr=new StringBuilder();
		for(String str :schemaList){
			schemaStr.append(str);
			if(count<schemaList.size()){
				schemaStr.append(",");
			}
			count++;
		}
		this.setInputLocation(inputLocation);
		this.setOutputLocation(outputLocation);
		this.setTransColumnIndex(indexValues.toString());
		this.setOutputSchema(colTransList);
		this.setInputSchema(schemaStr.toString());
/*		System.out.println(indexValues);
		System.out.println(colTransList);
		System.out.println(jsonObject);
		System.out.println("Output Schema :"+this.getOutputSchema());
		System.out.println("Input Param :"+this.getInputArgsString());*/
		
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public String getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(List<String> outputSchema) {
		
		StringBuilder schema=new StringBuilder();
		if(outputSchema !=null){
			int count=1;
			for(String str :outputSchema){
				schema.append(str);
				if(count<outputSchema.size()){
					schema.append(",");
				}
			}
		}
		this.outputSchema = schema.toString();
	}

	public String getInputSchema() {
		return inputSchema;
	}

	public void setInputSchema(String inputSchema) {
		this.inputSchema = inputSchema;
	}

	public String getTransColumnIndex() {
		return transColumnIndex;
	}

	public void setTransColumnIndex(String transColumnIndex) {
		this.transColumnIndex = transColumnIndex;
	}	
	
	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String getInputArgsString() {
		StringBuilder args = new StringBuilder();
		args.append(this.getInputLocation()).append(";");
		args.append(this.getOutputLocation()).append(";");
		args.append(this.getTransColumnIndex()).append(";");
		args.append(this.getOperator().replace(" ", "_"));
		if(this.getOperator().equalsIgnoreCase("Custom Value"))
			args.append(";").append(this.getValue());
		return args.toString();
	}

	@Override
	public String getInputDatasetSchema() {
		
		int count = 1;
		StringBuilder schemaStr = new StringBuilder();
		for (String str : schmeAndDatatypeList) {
			schemaStr.append(str);
			if (count < schmeAndDatatypeList.size()) {
				schemaStr.append(",");
			}
			count++;
		}
		return schemaStr.toString();
	}

	@Override
	public String getOutputDatasetSchema() {
		
		return this.outputSchema;
	}

}
