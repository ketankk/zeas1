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

public class MapReduceTransformation extends AbstractTransformation{
	
	private String projectName;
	private Long executionTime;
	private Entity entity;
	private JSONObject jsonObject;
	private String mapperClass;
	private String reducerClass;
	private String outputValueClass;
	private String outputKeyClass;
	private String mapredJarPath;
	private String outputSchema;
	private String inputSplits;

	//private String reducerClass;

	public MapReduceTransformation(Entity entity,JSONObject jsonObject,String projectName,Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
		init();
	}
	
	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(String reducerClass) {
		this.reducerClass = reducerClass;
	}

	public String getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(String outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

	public String getOutputKeyClass() {
		return outputKeyClass;
	}

	public void setOutputKeyClass(String outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	public String getMapredJarPath() {
		return mapredJarPath;
	}

	public void setMapredJarPath(String mapredJarPath) {
		this.mapredJarPath = mapredJarPath;
	}

		//parse the json and assign the values for respective  info.
		private void init() throws Exception {
			try {
				String stageName=jsonObject.getString("stageName");
				super.setId(stageName);
				ObjectMapper transJsonObject = new ObjectMapper();
				JsonNode rootNode= transJsonObject.readTree(entity.getJsonblob());
				this.inputSplits=rootNode.has("inputSplits")?rootNode.get("inputSplits").getTextValue():"";
				JsonNode params=rootNode.get("params");
				JsonNode columnArr = params.path("columnList");
				Iterator<JsonNode> itr=columnArr.iterator();
				List<String> colTransList=new ArrayList<>();
				StringBuilder outSchema = new StringBuilder();
				while(itr.hasNext()){
					JsonNode  temp=itr.next();
					colTransList.add(temp.get("name").getTextValue());
					outSchema.append(temp.get("name").getTextValue()+":"+temp.get("dataType").getTextValue()+",");
				}
				this.setOutputSchema(outSchema.substring(0, outSchema.length()-1));
				this.setMapperClass(params.get("mapperClass").getTextValue());
				this.setReducerClass(params.get("reducerClass").getTextValue());
				this.setOutputKeyClass(params.get("outputKey").getTextValue());
				this.setOutputValueClass(params.get("outputValue").getTextValue());
				this.setMapredJarPath(params.get("MRjarPath").getTextValue());
				
				String inputDataset=jsonObject.getString("input");
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
				
				String inputLocation="";
				this.setId(jsonObject.getString("stageName"));
				String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName");
				AbstractTransformation stage=TransformationUtil.getTransformationProperties(inputDataset);
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
				
				TransformationUtil.addTransformationProperties(this.getId(), this);
				this.setInputLocation(inputLocation);
				this.setOutputLocation(outputLocation);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	@Override
	public String getInputArgsString() {
		return null;
	}

	@Override
	public String getInputDatasetSchema() {
		return null;
	}

	@Override
	public String getOutputDatasetSchema() {
		return null;
	}

	public String getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(String mapperClass) {
		this.mapperClass = mapperClass;
	}

	public String getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(String outputSchema) {
		this.outputSchema = outputSchema;
	}

}
