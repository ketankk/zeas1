package com.itc.zeas.v2.pipeline;

import java.util.HashMap;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import org.json.JSONObject;

public class TransformationUtil {

	private static Map<String,AbstractTransformation> transNameAndProperties=new HashMap<>();

	public static Stage getColumnSelectorStage(Entity entity, JSONObject jsonObject, String projName, Long executionTime) throws Exception {

		Stage stage = new Stage();
		ColumnFilterTransformation columnFilterTransformation= new ColumnFilterTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(columnFilterTransformation);
		stage.setStageType(columnFilterTransformation.getType());
		stage.setName(columnFilterTransformation.getId());
		return stage;
	}

	public static Stage getColumnMissingDataStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		ColumnMissingDataTransformation columnMissingDataTransformation= new ColumnMissingDataTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(columnMissingDataTransformation);
		stage.setStageType(columnMissingDataTransformation.getType());
		stage.setName(columnMissingDataTransformation.getId());
		return stage;
	}

	public static Stage getHiveStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime){

		Stage stage = new Stage();
		HiveQueryTransformation hiveTransformation= new HiveQueryTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(hiveTransformation);
		stage.setStageType(hiveTransformation.getType());
		stage.setName(hiveTransformation.getId());
		stage.setHiveScript(hiveTransformation.getQuery());
		return stage;
	}
	
	public static Stage getPigStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		PigScriptTransformation pigTransformation= new PigScriptTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(pigTransformation);
		stage.setStageType(pigTransformation.getType());
		stage.setName(pigTransformation.getId());
		stage.setPigScript(pigTransformation.getScript());
		return stage;
	}

	public static Stage getJOINStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		JoinTransformation joinTransformation= new JoinTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(joinTransformation);
		stage.setStageType(joinTransformation.getType());
		stage.setName(joinTransformation.getId());
		return stage;
	}
	
	public static Stage getPartitionStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		PartitionTransformation partitionTransformation= new PartitionTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(partitionTransformation);
		stage.setStageType(partitionTransformation.getType());
		stage.setName(partitionTransformation.getId());
		return stage;
	}
	
	public static Stage getSubSetStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		SubsetTransformation subSetTransformation= new SubsetTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(subSetTransformation);
		stage.setStageType(subSetTransformation.getType());
		stage.setName(subSetTransformation.getId());
		return stage;
	}
	
	public static Stage getCompareModelStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) {

		Stage stage = new Stage();
		CompareModel compareModel= new CompareModel(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(compareModel);
		stage.setStageType(compareModel.getType());
		stage.setName(compareModel.getId());
		return stage;
	}
	

	public static AbstractTransformation getTransformationProperties(String transName) {

		return transNameAndProperties.get(transName);
	}

	public static void addTransformationProperties(String transName,AbstractTransformation stage) {

		transNameAndProperties.put(transName, stage);
	}

	public static void cleanTransformationProperties(String transName) {
		transNameAndProperties.remove(transName);
	}

	public static Stage getTrainModelStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) {

		Stage stage = new Stage();
		TrainModel trainModel= new TrainModel(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(trainModel);
		stage.setStageType(trainModel.getType());
		stage.setName(trainModel.getId());
		return stage;
	}
	
	public static Stage getTestModelStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		TestModel testModel= new TestModel(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(testModel);
		stage.setStageType(testModel.getType());
		stage.setName(testModel.getId());
		return stage;
	}
	
	public static Stage getMapRedStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		MapReduceTransformation mrTransformation= new MapReduceTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(mrTransformation);
		stage.setStageType(mrTransformation.getType());
		stage.setName(mrTransformation.getId());
		return stage;
	}
	
	public static Stage getGroupByStage(Entity entity,JSONObject jsonObject,String projName,Long executionTime) throws Exception {

		Stage stage = new Stage();
		GroupByTransformation groupByTransformation= new GroupByTransformation(entity, jsonObject,projName,executionTime);
		stage.setAbsTransformation(groupByTransformation);
		stage.setStageType(groupByTransformation.getType());
		stage.setName(groupByTransformation.getId());
		return stage;
	}
	
}
