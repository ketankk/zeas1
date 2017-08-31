package com.itc.zeas.v2.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.itc.zeas.profile.model.Entity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.itc.zeas.utility.DBUtility;

public class PartitionTransformation extends AbstractTransformation {

	private String outputSchema;
	private String inputSchema;
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private Long executionTime;
	private String percentOfData;
	private String inputSplits;
	
	public PartitionTransformation(Entity entity,JSONObject jsonObject,String projectName,Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
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
			this.setPercentOfData(params.has("percentage")?params.get("percentage").getTextValue():"0");
			String inputDataset=jsonObject.getString("input");
			Object[] arr= new HashSet<String>(Arrays.asList(inputDataset.split(","))).toArray();
			String[] inputs = Arrays.copyOf(arr, arr.length,String[].class);
			
			String tableType="entity";
			String inputLocation="";
			String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName")+"_split1"+","+
					ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName")+"_split2";
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
	public String getPercentOfData() {
		return percentOfData;
	}
	public void setPercentOfData(String percentOfData) {
		this.percentOfData = percentOfData;
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

	public void setExecutionTime(Long executionTime) {
		this.executionTime = executionTime;
	}
	
	@Override
	public String getInputArgsString() {
		StringBuilder args = new StringBuilder();
		args.append(this.getInputLocation()).append(";");
		args.append(this.getOutputLocation()).append(";");
		args.append(this.getPercentOfData());
		return args.toString();
	}

	@Override
	public String getInputDatasetSchema() {
		return "Not_Applicable";
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}
	


}
