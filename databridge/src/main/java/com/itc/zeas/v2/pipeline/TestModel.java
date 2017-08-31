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

public class TestModel extends AbstractTransformation {

	private String outputSchema = "";
	private String inputSchema;
	private String featuresIndex; // features column index comma sepearted
	private String labelIndex; // label column index
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private List<String> schemaAndDatatypeList;
	//private List<String> trainSchemaAndDatatypeList;
	private Long executionTime;
	private String algoType;
	private String modelLocation;

	public TestModel(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) throws Exception {
		super(entity.getType());
		this.jsonObject = jsonObject;
		this.entity = entity;
		this.projectName = "p_" + projectName;
		this.executionTime = executionTime;
		schemaAndDatatypeList = new ArrayList<>();
		init();
	}

	// parse the json and assign the values for respective info.
	private void init() throws Exception {
		try {
			String inputLocation = "";
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode = transJsonObject.readTree(entity.getJsonblob());
			JsonNode colAttrs = rootNode.get("params");
			List<String> featuresList = new ArrayList<>();
			List<String> schemaList = new ArrayList<>();
			String label="";
			// String colTransList=colAttrs.get("cloumnList").asText();
			String inputDataset = jsonObject.getString("input");
			String stageName = jsonObject.getString("stageName");
			Object[] arr= new HashSet<String>(Arrays.asList(inputDataset.split(","))).toArray();
			String[] inputs = Arrays.copyOf(arr, arr.length,String[].class);
			JsonNode datasetRootNode = null;
			super.setId(stageName);
			List<Entity> dbEntities = new ArrayList<Entity>();
			for (String input : inputs) {
				int datasetId = Integer.parseInt(input.split("-")[0]);
				Entity datasetEntity = null;
				datasetEntity = DBUtility.getEntityById(datasetId);
				String tableType = "entity";
				if (datasetEntity == null || datasetEntity.getId() == 0) {
					datasetEntity = DBUtility.getTransformationDetails(input);
					tableType = "module";
				}
				dbEntities.add(datasetEntity);
				String datasetJson = datasetEntity.getJsonblob();
				ObjectMapper datasetMapper = new ObjectMapper();
				if (tableType.equalsIgnoreCase("module")) {
					if (datasetEntity.getType().equalsIgnoreCase("Train")) {
						JsonNode tempRootNode = transJsonObject.readTree(datasetEntity.getJsonblob());
						JsonNode tempColAttrs = tempRootNode.get("params");
						algoType = tempColAttrs.get("algorithm").getTextValue();
						if (!this.algoType.equalsIgnoreCase("KMeans Clustering")) {
							JsonNode labelCol = tempColAttrs.path("label");
							label = labelCol.get("name").getTextValue();
						}
						JsonNode columnArr = tempColAttrs.path("features");
						Iterator<JsonNode> itr = columnArr.iterator();
						while (itr.hasNext()) {
							JsonNode temp = itr.next();
							/*System.out.println("temp : " + temp);
							System.out.println("temp.get(name) : " + temp.get("name"));*/
							String val = temp.get("name").getTextValue();
							featuresList.add(temp.get("name").getTextValue());
						}
						AbstractTransformation stage = TransformationUtil.getTransformationProperties(input);
						modelLocation = stage.getOutputLocation();
					} else {
						JsonNode tempRootNode = transJsonObject.readTree(datasetEntity.getJsonblob());
						JsonNode tempColAttrs = tempRootNode.get("params");
						JsonNode colList = tempColAttrs.get("columnList");
						Iterator<JsonNode> colListItr = colList.iterator();
						while (colListItr.hasNext()) {
							JsonNode temp = colListItr.next();
							schemaList.add(temp.get("name").getTextValue());
							schemaAndDatatypeList
									.add(temp.get("name").getTextValue() + ":" + temp.get("dataType").getTextValue());
						}
						AbstractTransformation stage = TransformationUtil.getTransformationProperties(input);
						if (stage != null) {
							inputLocation = stage.getOutputLocation();
						} else {
							inputLocation = datasetRootNode.get("location").asText() + "/cleansed";
						}
					}
				} else {
					datasetRootNode = datasetMapper.readTree(datasetJson);
					String schemaName = datasetRootNode.get("Schema").asText();
					schemaList = DBUtility.getColumns(schemaName);
					schemaAndDatatypeList = DBUtility.getColumnAndDatatype(schemaName);
					AbstractTransformation stage = TransformationUtil.getTransformationProperties(input);
					if (stage != null) {
						inputLocation = stage.getOutputLocation();
					} else {
						inputLocation = datasetRootNode.get("location").asText() + "/cleansed";
					}
				}
			}
			// String schemaName=datasetRootNode.get("Schema").asText();
			// List<String> schemaList=DBUtility.getColumns(schemaName);
			this.setId(jsonObject.getString("stageName"));
			String outputLocation = ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION + "/" + projectName + "/"
					+ executionTime + "/" + jsonObject.getString("stageName");
			this.setOutputLocation(outputLocation);
			TransformationUtil.addTransformationProperties(this.getId(), this);
			// String[] colArr=colTransList.split(",");
			StringBuilder featureIndVal = new StringBuilder();
			int count = 1;
			for (String str : featuresList) {
				int index = schemaList.indexOf(str.trim());
				featureIndVal.append(index);
				if (count < featuresList.size()) {
					featureIndVal.append(",");
				}
				count++;
			}

			count = 1;
			StringBuilder schemaStr = new StringBuilder();
			for (String str : schemaList) {
				schemaStr.append(str);
				if (count < schemaList.size()) {
					schemaStr.append(",");
				}
				count++;
			}

			count = 1;
			if (!this.algoType.equalsIgnoreCase("KMeans Clustering")) {
				String labelIndVal = "" + schemaList.indexOf(label.trim());
				this.setLabelIndex(labelIndVal);
			}

			this.setInputLocation(inputLocation);
			this.setOutputLocation(outputLocation);
			this.setFeaturesIndex(featureIndVal.toString());
			this.setInputSchema(schemaStr.toString());
			/*System.out.println(
					"****************TEST*********************************************************************************");
			System.out.println("schemaStr.toString() = " + schemaStr.toString());
			System.out.println("labelIndVal = " + labelIndVal);
			System.out.println("featuresList = " + featuresList);
			System.out.println("inputLocation = " + inputLocation);
			System.out.println("outputLocation = " + outputLocation);
			System.out.println("featuresIndex = " + this.featuresIndex);
			System.out.println("algoType = " + this.algoType);
			System.out.println(jsonObject);
			System.out.println("model Location= " + this.modelLocation);
			System.out.println(
					"*************************************************************************************************");*/

		} catch (

		JsonProcessingException e)

		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (

		IOException e)

		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

	public String getFeaturesIndex() {
		return featuresIndex;
	}

	public void setFeaturesIndex(String featuresIndex) {
		this.featuresIndex = featuresIndex;
	}

	public String getLabelIndex() {
		return labelIndex;
	}

	public void setLabelIndex(String labelIndex) {
		this.labelIndex = labelIndex;
	}

	public List<String> getSchemaAndDatatypeList() {
		return schemaAndDatatypeList;
	}

	public void setSchemaAndDatatypeList(List<String> schemaAndDatatypeList) {
		this.schemaAndDatatypeList = schemaAndDatatypeList;
	}

	@Override
	public String getInputArgsString() {
		StringBuilder args = new StringBuilder();
		args.append(this.getInputLocation()).append(";");
		args.append(this.getOutputLocation()).append(";");
		args.append(this.getAlgoType().replace(" ", "_")).append(";");
		args.append(this.getModelLocation()).append(";");
		args.append(this.getLabelIndex()).append(";");
		args.append(this.getFeaturesIndex()).append(";");
		return args.toString();
	}

	@Override
	public String getInputDatasetSchema() {

		int count = 1;
		StringBuilder schemaStr = new StringBuilder();
		for (String str : schemaAndDatatypeList) {
			schemaStr.append(str);
			if (count < schemaAndDatatypeList.size()) {
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

	public String getAlgoType() {
		return algoType;
	}

	public void setAlgoType(String algoType) {
		this.algoType = algoType;
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}

	public String getModelLocation() {
		return modelLocation;
	}

	public void setModelLocation(String modelLocation) {
		this.modelLocation = modelLocation;
	}

}
