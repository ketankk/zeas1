package com.itc.zeas.v2.pipeline;

import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.DBUtility;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class CompareModel extends AbstractTransformation {

	private JSONObject jsonObject;
	private String projectName;
	private Long executionTime;
	private String algoType;
	private String evalLocations = "";

	public CompareModel(Entity entity, JSONObject jsonObject, String projectName, Long executionTime) {
		super(entity.getType());
		this.jsonObject = jsonObject;
		this.projectName = "p_" + projectName;
		this.executionTime = executionTime;
		init();
	}

	// parse the json and assign the values for respective info.
	private void init() {
		try {
			String inputLocation = "";
			String inputDataset = jsonObject.getString("input");
			String stageName = jsonObject.getString("stageName");
			Object[] arr = new HashSet<String>(Arrays.asList(inputDataset.split(","))).toArray();
			String[] inputs = Arrays.copyOf(arr, arr.length, String[].class);
			super.setId(stageName);
			List<Entity> dbEntities = new ArrayList<Entity>();
			for (String input : inputs) {
				Entity datasetEntity = null;
				datasetEntity = DBUtility.getTransformationDetails(input);
				dbEntities.add(datasetEntity);
				AbstractTransformation stage = TransformationUtil.getTransformationProperties(input);
				if (this.evalLocations.isEmpty()) {
					this.evalLocations = this.evalLocations.concat(stage.getOutputLocation()).concat(";");
				} else {
					this.evalLocations = this.evalLocations.concat(stage.getOutputLocation());
				}
			}
			this.setId(jsonObject.getString("stageName"));
			String outputLocation = ProjectConstant.HDFS_TRANSF_OUTPUT_LOCATION + "/" + projectName + "/"
					+ executionTime + "/" + jsonObject.getString("stageName");
			this.setOutputLocation(outputLocation);
			TransformationUtil.addTransformationProperties(this.getId(), this);
			this.setInputLocation(inputLocation);
			this.setOutputLocation(outputLocation);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getInputArgsString() {
		return this.getOutputLocation();
	}

	@Override
	public String getInputDatasetSchema() {

		return this.evalLocations;
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
		return "";
	}


}
