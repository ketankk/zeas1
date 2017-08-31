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

public class GroupByTransformation extends AbstractTransformation {

	private String outputSchema;
	private String inputSchema;
	//private String transColumnIndex; //transformation column index comma sepearted
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private List<String> schmeAndDatatypeList;
	private Long executionTime;
	private String groupByColumns;
	private String selectColumns;
	private String inputSplits;
	
	public String getInputSplits() {
		return inputSplits;
	}

	public void setInputSplits(String inputSplits) {
		this.inputSplits = inputSplits;
	}

	public GroupByTransformation(Entity entity,JSONObject jsonObject,String projectName,Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject=jsonObject;
		this.entity=entity;
		this.projectName="p_"+projectName;
		this.executionTime=executionTime;
		schmeAndDatatypeList=new ArrayList<>();
		groupByColumns = "";
		selectColumns = "";
		init();
	}

	//parse the json and assign the values for respective  info.
	private void init() throws Exception {
		try {
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode= transJsonObject.readTree(entity.getJsonblob());
			JsonNode colAttrs=rootNode.get("params");
			JsonNode columnArr = colAttrs.path("columnList");
			this.setInputSplits(colAttrs.has("inputSplits")?"_"+colAttrs.get("inputSplits").getTextValue():"");
			Iterator<JsonNode> itr=columnArr.iterator();
			StringBuilder outSchema = new StringBuilder();
			while(itr.hasNext()){
				JsonNode  temp=itr.next();
				outSchema.append(temp.get("name").getTextValue()+":"+temp.get("dataType").getTextValue()+",");
			}
			this.outputSchema = outSchema.substring(0, outSchema.length()-1);

			//Iterate over GroupBy Columns array in JSON
			JsonNode groupByArr = colAttrs.path("groupby");
			Iterator<JsonNode> groupByItr=groupByArr.iterator();
			while(groupByItr.hasNext()){
				JsonNode column = groupByItr.next();
				groupByColumns += column.getTextValue()+",";
			}

			//Iterate over Selected columns array in JSON - Aggregation
			JsonNode aggr = colAttrs.path("oparation");
			Iterator<JsonNode> aggrItr=aggr.iterator();
			while(aggrItr.hasNext()){
				JsonNode column = aggrItr.next();
				if(column.get("oparation").getTextValue().equalsIgnoreCase("None")){
				    continue;
				}
				selectColumns += column.get("name").getTextValue()+":"+column.get("oparation").getTextValue()+",";
			}

			//String colTransList=colAttrs.get("cloumnList").asText();
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
			//String schemaName=datasetRootNode.get("Schema").asText();
			//List<String> schemaList=DBUtility.getColumns(schemaName);
			String inputLocation="";
			this.setId(jsonObject.getString("stageName"));
			String outputLocation=ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION+"/"+projectName+"/"+executionTime+"/"+jsonObject.getString("stageName");
			AbstractTransformation stage=TransformationUtil.getTransformationProperties(inputDataset);
			if( stage !=null) {
				inputLocation=stage.getOutputLocation();
				if(inputLocation.contains(",") && (!this.getInputSplits().isEmpty())){
				inputLocation=this.getInputSplits().equalsIgnoreCase("split1")?inputLocation.split(",")[0]:inputLocation.split(",")[1];
				this.setOutputLocation(outputLocation);
				}
			}else{
				inputLocation=datasetRootNode.get("location").asText()+"/cleansed";
				this.setOutputLocation(outputLocation);
			}

			TransformationUtil.addTransformationProperties(this.getId(), this);

			int count=1;		
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
			this.setInputSchema(schemaStr.toString());
			System.out.println(jsonObject);

		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

	@Override
	//inputPath;outputPath;groupedcolumn;aggregationfield:aggregationType
	public String getInputArgsString() {
		StringBuilder args = new StringBuilder();
		args.append(this.getInputLocation()).append(";");
		args.append(this.getOutputLocation()).append(";");
		args.append(this.groupByColumns.substring(0, groupByColumns.length()-1)).append(";");
		args.append(this.selectColumns.substring(0, selectColumns.length()-1));
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

	public Long getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(Long executionTime) {
		this.executionTime = executionTime;
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}

}
