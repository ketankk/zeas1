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

public class TrainModel extends AbstractTransformation {

	private String outputSchema = "";
	private String inputSchema;
	private String featuresIndex; // features column index comma sepearted
	private String labelIndex; // label column index
	private String stepSize;
	private String numIterations;
	private Entity entity;
	private JSONObject jsonObject;
	private String projectName;
	private List<String> schemaAndDatatypeList;
	private Long executionTime;
	private String algoType;
	private String minBatchFraction;
	private Boolean intercept;
	private String regParam;
	private String numOfClasses;
	private String trees;
	private String depth;
	private String bins;
	private String seed;
	private String subset;
	private String impurity;

	public TrainModel(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) {
		super(entity.getType());
		this.jsonObject = jsonObject;
		this.entity = entity;
		this.projectName = "p_" + projectName;
		this.executionTime = executionTime;
		schemaAndDatatypeList = new ArrayList<>();
		init();
	}

	// parse the json and assign the values for respective info.
	private void init() {
		try {
			String inputLocation = "";
			ObjectMapper transJsonObject = new ObjectMapper();
			JsonNode rootNode = transJsonObject.readTree(entity.getJsonblob());
			JsonNode colAttrs = rootNode.get("params");
			JsonNode columnArr = colAttrs.path("features");
			Iterator<JsonNode> itr = columnArr.iterator();
			List<String> featuresList = new ArrayList<>();
			List<String> schemaList = new ArrayList<>();
			while (itr.hasNext()) {
				JsonNode temp = itr.next();
				/*
				 * System.out.println("temp : " + temp); System.out.println(
				 * "temp.get(name) : " + temp.get("name"));
				 */ featuresList.add(temp.get("name").getTextValue());
			}

			// String colTransList=colAttrs.get("cloumnList").asText();
			String inputDataset = jsonObject.getString("input");
			String stageName = jsonObject.getString("stageName");
			Object[] arr = new HashSet<String>(Arrays.asList(inputDataset.split(","))).toArray();
			String[] inputs = Arrays.copyOf(arr, arr.length, String[].class);
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
					JsonNode tempRootNode = transJsonObject.readTree(datasetEntity.getJsonblob());
					JsonNode tempColAttrs = tempRootNode.get("params");
					if (datasetEntity.getType().equalsIgnoreCase("Linear Regression")) {
						this.algoType = datasetEntity.getType();
						this.stepSize = tempColAttrs.get("stepsize").getTextValue();
						this.numIterations = tempColAttrs.get("iteration").getTextValue();
						this.intercept = new Boolean(tempColAttrs.get("intercept").getTextValue().toLowerCase());
						this.minBatchFraction = tempColAttrs.get("minBatchFraction").getTextValue();
					} else if (datasetEntity.getType().equalsIgnoreCase("Binary Logistic Regression")) {
						this.algoType = datasetEntity.getType();
						this.stepSize = tempColAttrs.get("stepsize").getTextValue();
						this.numIterations = tempColAttrs.get("iteration").getTextValue();
						this.intercept = new Boolean(tempColAttrs.get("intercept").getTextValue().toLowerCase());
						this.regParam = tempColAttrs.get("regParam").getTextValue();
						this.minBatchFraction = tempColAttrs.get("minBatchFraction").getTextValue();
					} else if (datasetEntity.getType().equalsIgnoreCase("Multiclass Logistic Regression")) {
						this.algoType = datasetEntity.getType();
						this.numIterations = tempColAttrs.get("iteration").getTextValue();
						this.intercept = new Boolean(tempColAttrs.get("intercept").getTextValue().toLowerCase());
						this.regParam = tempColAttrs.get("regParam").getTextValue();
					} else if (datasetEntity.getType().equalsIgnoreCase("KMeans Clustering")) {
						this.algoType = datasetEntity.getType();
						this.numIterations = tempColAttrs.get("iteration").getTextValue();
						this.numOfClasses = tempColAttrs.get("numofclasses").getTextValue();
					} else if (datasetEntity.getType().equalsIgnoreCase("Random Forest Regression")
							|| datasetEntity.getType().equalsIgnoreCase("Random Forest Classification")) {
						algoType = datasetEntity.getType();
						this.trees = tempColAttrs.get("trees").getTextValue().toLowerCase();
						this.depth = tempColAttrs.get("depth").getTextValue().toLowerCase();
						this.bins = tempColAttrs.get("bins").getTextValue().toLowerCase();
						this.seed = tempColAttrs.get("seed").getTextValue().toLowerCase();
						this.subset = tempColAttrs.get("subset").getTextValue().toLowerCase();
						this.impurity = tempColAttrs.get("impurity").getTextValue().toLowerCase();
					} else {
						JsonNode colList = tempColAttrs.get("columnList");
						Iterator<JsonNode> colListItr = colList.iterator();
						while (colListItr.hasNext()) {
							JsonNode temp = colListItr.next();
							schemaList.add(temp.get("name").getTextValue());
							schemaAndDatatypeList
									.add(temp.get("name").getTextValue() + ":" + temp.get("dataType").getTextValue());
						}
						AbstractTransformation stage = TransformationUtil.getTransformationProperties(input);
						inputLocation = stage.getOutputLocation();
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
					System.out.println("Location + " + datasetRootNode.get("location"));
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
			// System.out.println("lable + " + label);

			if (!this.algoType.equalsIgnoreCase("KMeans Clustering")) {
				JsonNode labelCol = colAttrs.path("label");
				String labelIndVal = "" + schemaList.indexOf(labelCol.get("name").getTextValue().trim());
				this.setLabelIndex(labelIndVal);
			}

			this.setInputLocation(inputLocation);
			this.setOutputLocation(outputLocation);
			this.setFeaturesIndex(featureIndVal.toString());
			this.setInputSchema(schemaStr.toString());
/*
			System.out.println("***************************************Train*****************************************");
			System.out.println("schemaStr.toString() = " + schemaStr.toString());
			System.out.println("featuresList = " + featuresList);
			System.out.println("inputLocation = " + inputLocation);
			System.out.println("outputLocation = " + outputLocation);
			System.out.println("stepSize = " + this.stepSize);
			System.out.println("numIterations = " + this.numIterations);
			System.out.println("featuresIndex = " + this.featuresIndex);
			System.out.println("labelIndex = " + this.labelIndex);
			System.out.println("Intercept = " + this.intercept);
			System.out.println("MIn Batch fraction = " + this.minBatchFraction);
			System.out.println("regParam = " + this.regParam);
			System.out.println("algoType = " + this.algoType);
			System.out.println("args  = " + this.getInputArgsString());
			System.out.println(jsonObject);
			System.out.println("***************************************Train*****************************************");*/

		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
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

	public String getStepSize() {
		return stepSize;
	}

	public void setStepSize(String stepSize) {
		this.stepSize = stepSize;
	}

	public String getNumIterations() {
		return numIterations;
	}

	public void setNumIterations(String numIterations) {
		this.numIterations = numIterations;
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
		if (this.getAlgoType().equalsIgnoreCase("KMeans Clustering")) {
			args.append(this.getFeaturesIndex()).append(";");
			args.append(this.numOfClasses).append(";");
			args.append(this.numIterations);
		} else {
			args.append(this.getLabelIndex()).append(";");
			args.append(this.getFeaturesIndex()).append(";");
			if (this.getAlgoType().equalsIgnoreCase("Linear Regression")) {
				args.append(this.getStepSize()).append(";");
				args.append(this.getNumIterations()).append(";");
				args.append(this.getMinBatchFraction()).append(";");
				args.append(this.getIntercept());
			} else if (this.getAlgoType().equalsIgnoreCase("Binary Logistic Regression")) {
				args.append(this.getStepSize()).append(";");
				args.append(this.getNumIterations()).append(";");
				args.append(this.getMinBatchFraction()).append(";");
				args.append(this.getRegParam()).append(";");
				args.append(this.getIntercept());
			} else if (this.getAlgoType().equalsIgnoreCase("Multiclass Logistic Regression")) {
				args.append(this.getNumIterations()).append(";");
				args.append(this.getRegParam()).append(";");
				args.append(this.getIntercept());
			} else if (this.getAlgoType().equalsIgnoreCase("Random Forest Regression")
					|| this.getAlgoType().equalsIgnoreCase("Random Forest Classification")) {
				args.append(this.trees).append(";");
				args.append(this.depth).append(";");
				args.append(this.bins).append(";");
				args.append(this.subset).append(";");
				args.append(this.impurity).append(";");
				args.append(this.seed);
			}
		}
		// System.out.println("args String : ========= " + args.toString());
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

	public String getMinBatchFraction() {
		return minBatchFraction;
	}

	public void setMinBatchFraction(String minBatchFraction) {
		this.minBatchFraction = minBatchFraction;
	}

	public Boolean getIntercept() {
		return intercept;
	}

	public void setIntercept(Boolean intercept) {
		this.intercept = intercept;
	}

	public String getRegParam() {
		return regParam;
	}

	public void setRegParam(String regParam) {
		this.regParam = regParam;
	}

	public String getNumOfClasses() {
		return numOfClasses;
	}

	public void setNumOfClasses(String numOfClasses) {
		this.numOfClasses = numOfClasses;
	}

	public String getTrees() {
		return trees;
	}

	public void setTrees(String trees) {
		this.trees = trees;
	}

	public String getDepth() {
		return depth;
	}

	public void setDepth(String depth) {
		this.depth = depth;
	}

	public String getBins() {
		return bins;
	}

	public void setBins(String bins) {
		this.bins = bins;
	}

	public String getSeed() {
		return seed;
	}

	public void setSeed(String seed) {
		this.seed = seed;
	}

	public String getSubset() {
		return subset;
	}

	public void setSubset(String subset) {
		this.subset = subset;
	}

	public String getImpurity() {
		return impurity;
	}

	public void setImpurity(String impurity) {
		this.impurity = impurity;
	}

	@Override
	public String getOutputDatasetSchema() {
		return this.outputSchema;
	}

}
