package com.itc.zeas.v2.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.itc.zeas.utility.DBUtility;

public class JoinTransformation extends AbstractTransformation {

	private String outputSchema;
	private String inputSchema;
	/**
	 * Column indexes of columns from both data sets. Ex -
	 * joinIndexes1;joinIndexes2
	 */
	private String joinIndexes;
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	// private List<String> schmeAndDatatypeList;
	private Long executionTime;
	private String inputSplits;

	public JoinTransformation(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject = jsonObject;
		this.entity = entity;
		this.projectName = "p_" + projectName;
		this.executionTime = executionTime;
		// schmeAndDatatypeList=new ArrayList<>();
		init();
	}

	// parse the json and assign the values for respective info.
	private void init() throws Exception {
		try {
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode = transJsonObject.readTree(entity.getJsonblob());
			JsonNode colAttrs = rootNode.get("params");
			this.inputSplits = rootNode.has("inputSplits") ? rootNode.get("inputSplits").getTextValue() : "";
			JsonNode columnArr = colAttrs.path("columnList");
			Iterator<JsonNode> itr = columnArr.iterator();
			List<String> colTransList = new ArrayList<>();
			StringBuilder outSchema = new StringBuilder();
			while (itr.hasNext()) {
				JsonNode temp = itr.next();
				colTransList.add(temp.get("name").getTextValue());
				outSchema.append(temp.get("name").getTextValue() + ":" + temp.get("dataType").getTextValue() + ",");
			}
			this.outputSchema = outSchema.substring(0, outSchema.length() - 1);
			/**
			 * Set Join column indexes from both inputs
			 */
			this.setJoinIndexes(this.getJoinColumnIndices(colAttrs));

			// String colTransList=colAttrs.get("cloumnList").asText();
			String inputDataset = jsonObject.getString("input");
			String stageName = jsonObject.getString("stageName");
			super.setId(stageName);

			String outputLocation = ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION + "/" + projectName + "/"
					+ executionTime + "/" + jsonObject.getString("stageName");
			/**
			 * Get the profile id's for Table1 and Table 3 to use them to set
			 * the Table1 path first and Table3 Path second while creating the
			 * Oozie workflow arguments.
			 */
			JsonNode datasetsAttrib = colAttrs.get("dataset");
			Map<String, String> idTableMap = new HashMap<String, String>();
			Iterator<JsonNode> datasetsAttribItr = datasetsAttrib.iterator();
			while (datasetsAttribItr.hasNext()) {
				JsonNode idTableLinkNode = datasetsAttribItr.next();
				idTableMap.put(idTableLinkNode.get("id").getTextValue().replace("_", "-"),
						idTableLinkNode.get("tableName").getTextValue());
			}
			String tableOneInputLoc = "";
			String tableTwoInputLoc = "";

			this.setOutputLocation(outputLocation);
			String[] datasetId = inputDataset.split(",");
			for (int i = 0; i < datasetId.length; i++) {
				System.out.println("datasetId.length " + datasetId.length);
				String inputLocation = "";
				int id = Integer.parseInt(datasetId[i].split("-")[0]);
				Entity datasetEntity = null;
				datasetEntity = DBUtility.getEntityById(id);
				String tableType = "entity";
				if (datasetEntity == null || datasetEntity.getId() == 0) {
					datasetEntity = DBUtility.getTransformationDetails(datasetId[i]);
					tableType = "module";
				}
				String datasetJson = datasetEntity.getJsonblob();
				ObjectMapper datasetMapper = new ObjectMapper();
				JsonNode datasetRootNode = datasetMapper.readTree(datasetJson);

				this.setId(jsonObject.getString("stageName"));
				AbstractTransformation stage = TransformationUtil.getTransformationProperties(datasetId[i]);
				if (stage != null) {
					inputLocation = stage.getOutputLocation();
					if (inputLocation.contains(",") && (!this.inputSplits.isEmpty())) {
						inputLocation = this.inputSplits.equalsIgnoreCase("split1") ? inputLocation.split(",")[0]
								: inputLocation.split(",")[1];
					}
				} else {
					inputLocation = datasetRootNode.get("location").asText() + "/cleansed";

				}

				/**
				 * Reading the input location and assigning it to proper table
				 * paths for later use
				 */
				if (idTableMap.get(datasetId[i].trim()).equalsIgnoreCase("table1")) {
					tableOneInputLoc = inputLocation;
				} else if (idTableMap.get(datasetId[i]).equalsIgnoreCase("table3")) {
					tableTwoInputLoc = inputLocation;
				}

			}
			/**
			 * Setting the Input location
			 * sequentially(tableOneInputLoc;tableTwoInputLoc) to send it as
			 * argument in Oozie
			 */
			this.setInputLocation(tableOneInputLoc + ";" + tableTwoInputLoc);

			/*
			 * List<String> schemaList=new ArrayList<>();
			 * if(tableType.equalsIgnoreCase("module")) { JsonNode tempRootNode=
			 * transJsonObject.readTree(datasetEntity.getJsonblob()); JsonNode
			 * tempColAttrs=tempRootNode.get("params"); JsonNode
			 * colList=tempColAttrs.get("columnList"); Iterator<JsonNode>
			 * colListItr=colList.iterator(); while(colListItr.hasNext()){
			 * JsonNode temp=colListItr.next();
			 * schemaList.add(temp.get("name").getTextValue());
			 * schmeAndDatatypeList.add(temp.get("name").getTextValue()+":"+temp
			 * .get("dataType").getTextValue()); }
			 * 
			 * } else{ String schemaName=datasetRootNode.get("Schema").asText();
			 * schemaList=DBUtility.getColumns(schemaName);
			 * schmeAndDatatypeList=DBUtility.getColumnAndDatatype(schemaName);
			 * }
			 */
			// String schemaName=datasetRootNode.get("Schema").asText();
			// List<String> schemaList=DBUtility.getColumns(schemaName);

			TransformationUtil.addTransformationProperties(this.getId(), this);
			// String[] colArr=colTransList.split(",");
			/*
			 * StringBuilder indexValues=new StringBuilder(); int count=1;
			 * for(String str :colTransList){ int
			 * index=schemaList.indexOf(str.trim()); indexValues.append(index);
			 * if(count<colTransList.size()){ indexValues.append(","); }
			 * count++; }
			 * 
			 * count=1; StringBuilder schemaStr=new StringBuilder(); for(String
			 * str :schemaList){ schemaStr.append(str);
			 * if(count<schemaList.size()){ schemaStr.append(","); } count++; }
			 */

			this.setOutputLocation(outputLocation);
			// this.setTransColumnIndex(indexValues.toString());
			// this.setOutputSchema(colTransList);
			// this.setInputSchema(schemaStr.toString());
			// System.out.println(indexValues);
			// System.out.println(colTransList);
			System.out.println(jsonObject);

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
		StringBuilder args = new StringBuilder();
		args.append(this.getInputLocation()).append(";");
		args.append(this.getOutputLocation()).append(";");
		args.append(this.getJoinIndexes());
		return args.toString();
	}

	@Override
	public String getInputDatasetSchema() {
		return "NotApplicable";
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}

	public String getJoinIndexes() {
		return joinIndexes;
	}

	public void setJoinIndexes(String joinIndexes) {
		this.joinIndexes = joinIndexes;
	}

	private String getJoinColumnIndices(JsonNode rootNode) {
		String indices = "";
		indices += getIndex(rootNode, "table1");
		indices += ";" + getIndex(rootNode, "table3");
		return indices;
	}

	private String getIndex(JsonNode node, String attribute) {
		String index = "";
		JsonNode columnArr = node.path(attribute);
		Iterator<JsonNode> itr = columnArr.iterator();
		while (itr.hasNext()) {
			index += itr.next().asText() + ",";
		}
		return index.substring(0, index.length() - 1);
	}

}
