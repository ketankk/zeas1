package com.itc.zeas.v2.pipeline;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.project.model.ProjectEntity;
import com.itc.zeas.utility.utility.ConfigurationReader;

import org.apache.flume.source.SyslogUDPSource.syslogHandler;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.taphius.databridge.dao.IngestionLogDAO;
import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.itc.zeas.utility.DBUtility;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.pipeline.HiveClient;
import com.taphius.pipeline.JobPropertiesFileWriter;
import com.taphius.pipeline.Pipeline;
import com.taphius.pipeline.PipelineUtil;
import com.taphius.pipeline.WorkflowBuilder;
import com.zdp.dao.SearchCriteriaEnum;
import com.itc.zeas.project.model.SearchCriterion;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.project.extras.ZDPDaoConstant;

/**
 * This is executor class for Pipeline. This gets invoked for every pipeline RUN
 * click.
 * 
 * @author 16795
 * 
 */
public class PipelineExecutor implements Runnable {
	/**
	 * LOGGER for the class.
	 */
	public static Logger LOG = Logger.getLogger(PipelineExecutor.class);

	public static final String REG_TABLE_SCRIPT = "hive -e \"create table if not exists zeas.m_$4 row format delimited fields terminated by',' as select $5 from $6\"";

	public static final String TRANSFORM_SUBMIT_COMMAND = "#!/bin/sh\n"
			+ ConfigurationReader.getProperty("SPARK_SUBMIT")
			+ " --class com.zdp.transformations.RunTransformation --master yarn --deploy-mode client Transformation.jar $1 $2 $3";
	public static final String ML_SUBMIT_COMMAND = "#!/bin/sh\n" + ConfigurationReader.getProperty("SPARK_SUBMIT")
			+ " --class com.zeas.spark.mlib.RunMLAlgorithm --master yarn --deploy-mode client MachineLearningLib.jar $1 $2 $3";
	/**
	 * Represents Pipeline info
	 */
	private Pipeline pipeline;
	/**
	 * Name of the pipeline this thread will execute.
	 */
	private String pipelineName;
	/**
	 * project run id represent column id in project_histoy table
	 */
	private Long projectRunId;
	/**
	 * 
	 */
	private List<Stage> stages = new ArrayList<Stage>();

	/**
	 * 
	 * @param pipelineName
	 *            Name of the pipeline this thread will execute.
	 */
	// to store module execution timestamp value for project
	private Long executionTime;

	public PipelineExecutor(String pipelineName, Long projectRunId) {
		this.projectRunId = projectRunId;
		this.pipelineName = pipelineName;
	}

	@Override
	public void run() {
		try {
			Date date = new Date();
			executionTime = date.getTime();
			this.processPipeline(this.pipelineName, this.projectRunId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method to process/execute pipeline.
	 * 
	 * @param pipelineName
	 *            {@link String} name of the pipeline to Run
	 * @param projectRunId
	 * @throws SQLException
	 */
	public void processPipeline(String pipelineName, Long projectRunId) throws Exception {
		LOG.info("Going to process pipeline - " + pipelineName);
		LOG.info("projectRunId " + projectRunId);
		// Pipeline info used for log pipeline process in future.
		this.pipeline = this.getPipelineDetails(pipelineName);
		List<Stage> actions = this.getJoiningNodes(stages);

		WorkflowBuilder wfBuilder = new WorkflowBuilder();
		LOG.info("Creating Oozie workflow for -pipeline : " + pipelineName + " start action as -"
				+ actions.get(0).getName());
		Document workflowDoc = wfBuilder.getWorkFlowTemplate(pipelineName, "m_" + actions.get(0).getName());

		for (Stage stage : actions) {

			Element stageAction = new Element("m_" + stage.getName());
			System.out.println("stage.getStageType() =========" + stage.getStageType());

			if (stage.getStageType().equalsIgnoreCase(ProjectConstant.HIVE_TRANSFORMATION)) {

				HiveQueryTransformation ht = (HiveQueryTransformation) stage.absTransformation;

				// String pattern =
				// "from\\s+(?:\\w+\\.)*(\\w+)(\\s*$|\\s+(WHERE|JOIN|START\\s+WITH|ORDER\\s+BY|GROUP\\s+BY))";
				StringBuilder reg_parent_query = new StringBuilder();

				/**
				 * Logic to check if this Hive Action has UDF defined in it.If
				 * so needs to register UDF.
				 */
				if (ht.getHiveUdfJarPath() != null && !ht.getHiveUdfJarPath().isEmpty()) {
					String udfJarName = ht.getHiveUdfJarPath().substring(ht.getHiveUdfJarPath().lastIndexOf("/") + 1);
					String hdfsAppPath = ConfigurationReader.getProperty("HDFS_FQDN") + File.separator
							+ ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipelineName
							+ File.separator + "lib" + File.separator + udfJarName;

					// copy the udf jar file to pipeline workflow directory.
					PipelineUtil.copyJarFile(
							ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipelineName,
							ht.getHiveUdfJarPath());

					reg_parent_query.append("add jar " + hdfsAppPath + ";\n");
					reg_parent_query.append(ht.getDefineFuncs() + ";\n");
				}
				for (int i = 0; i < stage.getInput().length; i++) {
					AbstractTransformation t = TransformationUtil.getTransformationProperties(stage.getInput()[i]);
					if (null != t) {

						String values[] = ht.getTableMapping().get(t.getId()).split("\\|", -1);
						String outPutLocation = t.getOutputLocation();
						for (String value : values) {
							String nameAndSpilt[] = value.split(",", -1);// Table2,split
							String split = nameAndSpilt[1];
							String outLocation = outPutLocation;
							if (outPutLocation.contains(",") && (!split.isEmpty())) {
								outLocation = split.equalsIgnoreCase("split1") ? outPutLocation.split(",")[0]
										: outPutLocation.split(",")[1];
							}
							// Ignoring case sensitivity of table name, Table1,
							// Table2

							String name = nameAndSpilt[0].toLowerCase();
							String tableName = "m_" + t.getId().replace("-", "_") + split;

							stage.setHiveScript(stage.getHiveScript().replace(name, tableName));

							// If check is added to skip any stages of are of
							// Hive type.If Hive stages are there
							// we need not have to register them as Hive tables
							// again.
							if (!t.getType().equalsIgnoreCase(ProjectConstant.HIVE_TRANSFORMATION)) {
								reg_parent_query.append(HiveClient.getRegisterTblQuery(tableName,
										t.getOutputDatasetSchema(), outLocation));
							}
						}

						// stage.setHiveScript(stage.getHiveScript().replaceAll(pattern,
						// "from m_"+t.getId().replace("-", "_")));
					} else {
						String dataset = ht.getTableMapping().get(stage.getInput()[i]).split("\\|", -1)[0];
						int datasetId = Integer.parseInt(stage.getInput()[i].split("-")[0]);
						Entity datasetEntity = DBUtility.getEntityById(datasetId);
						String datasetName = datasetEntity.getName();
						stage.setHiveScript(
								stage.getHiveScript().replace(dataset.split(",", -1)[0].toLowerCase(), datasetName));
					}
				}

				stageAction = addHiveAction(stage, pipelineName, reg_parent_query.toString());
			} else if (stage.getStageType().equalsIgnoreCase(ProjectConstant.PIG_TRANSFORMATION)) {
				PigScriptTransformation ht = (PigScriptTransformation) stage.absTransformation;

				Map<String, AbstractTransformation> parrentTables = new HashMap<>();
				for (int i = 0; i < stage.getInput().length; i++) {
					AbstractTransformation t = TransformationUtil.getTransformationProperties(stage.getInput()[i]);
					if (null != t) {
						String values[] = ht.getTableMapping().get(t.getId()).split("\\|", -1);
						String outPutLocation = t.getOutputLocation();
						for (String value : values) {
							AbstractTransformation tn = new AbstractTransformation("Pig") {
								@Override
								public String getOutputDatasetSchema() {
									return null;
								}

								@Override
								public String getInputDatasetSchema() {
									return null;
								}

								@Override
								public String getInputArgsString() {
									return null;
								}
							};
							String nameAndSpilt[] = value.split(",", -1);// Table2,split
							String split = nameAndSpilt[1];
							tn.setDatasetSchema(
									PigScriptTransformation.getSchemaForPig(t.getOutputDatasetSchema()).toString());
							String outLocation = outPutLocation;
							if (outPutLocation.contains(",") && (!split.isEmpty())) {
								outLocation = split.equalsIgnoreCase("split1") ? outPutLocation.split(",")[0]
										: outPutLocation.split(",")[1];
							}
							tn.setOutputLocation(outLocation);
							parrentTables.put(nameAndSpilt[0], tn);
						}
					} else {
						t = new AbstractTransformation("Pig") {
							@Override
							public String getOutputDatasetSchema() {
								return null;
							}

							@Override
							public String getInputDatasetSchema() {
								return null;
							}

							@Override
							public String getInputArgsString() {
								return null;
							}
						};
						String dataset = ht.getTableMapping().get(stage.getInput()[i]).split("\\|", -1)[0];
						int datasetId = Integer.parseInt(stage.getInput()[i].split("-")[0]);
						Entity datasetEntity = DBUtility.getEntityById(datasetId);
						String dataSetPath = "";
						String schemaName = "";
						ObjectMapper mapper = new ObjectMapper();
						try {
							JsonNode rootNode = mapper.readTree(datasetEntity.getJsonblob());
							dataSetPath = rootNode.get("location").getTextValue();
							schemaName = rootNode.get("Schema").getTextValue();
						} catch (IOException e) {
						}
						DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(
								DataSchema.class);
						DataSchema schema = parser.getDSConfigDetails(DBUtility.getJSON_DATA(schemaName));
						final String datasetSchema = PigScriptTransformation.getSchemaForPig(schema.getDataAttribute())
								.toString();
						t.setDatasetSchema(datasetSchema);
						t.setOutputLocation(dataSetPath + "/cleansed");
						parrentTables.put(dataset.split(",", -1)[0], t);
					}
				}
				stageAction = addPigAction(stage, pipelineName, parrentTables);
			} else if (stage.getStageType().equalsIgnoreCase(ProjectConstant.FORK)) {
				stageAction = this.getForkNode(stage);
			} else if (stage.getStageType().equalsIgnoreCase(ProjectConstant.JOIN_NODE)) {
				stageAction = this.getJoinNode(stage);
			} else if (stage.getStageType().equalsIgnoreCase(ProjectConstant.TRAIN_MODEL_TRANSFORMATION)
					|| stage.getStageType().equalsIgnoreCase(ProjectConstant.TEST_MODEL_TRANSFORMATION)
					|| stage.getStageType().equalsIgnoreCase(ProjectConstant.COMPARE_MODEL_TRANSFORMATION)) {
				stageAction = addMachineLearningAction(stage, pipelineName);
			} else if (stage.getStageType().equalsIgnoreCase(ProjectConstant.MAPRED_TRANSFORMATION)) {
				stageAction = this.addMapRedAction(stage, pipelineName);
			} else {
				stageAction = addtransformationAction(stage, pipelineName);
			}
			workflowDoc.getRootElement().addContent(stageAction);
		}

		// clean the cache for stage
		for (Stage stage : actions) {
			TransformationUtil.cleanTransformationProperties(stage.getName());
		}

		wfBuilder.endWorkflowXML(workflowDoc.getRootElement());
		String pipelinePath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipelineName
				+ File.separator;
		wfBuilder.saveWorkFlowXML(workflowDoc, pipelinePath + "workflow.xml");

		// Generate job.properties file
		JobPropertiesFileWriter prop = new JobPropertiesFileWriter();
		prop.jobPropertiesWriter(pipelineName);

		// Copy workflow.xml,script and properties file to HDFS for Oozie
		// execution
		ZDPDataAccessObjectImpl accessObject = new ZDPDataAccessObjectImpl();
		String prId = pipelineName.split("-")[0];
		System.out.println("Project id:**************" + prId);
		SearchCriterion cr = new SearchCriterion("id", prId.toString(), SearchCriteriaEnum.EQUALS);
		List<SearchCriterion> crList = new ArrayList<>();
		crList.add(cr);
		ProjectEntity projectProjectEntity = null;
		try {
			projectProjectEntity = accessObject.findExactObject(ZDPDaoConstant.ZDP_PROJECT_TABLE, crList);
		} catch (Exception e) {
		}
		String prName = "";
		if (projectProjectEntity != null && projectProjectEntity.getId() > 0) {
			prName = projectProjectEntity.getName();
		}
		System.out.println("Project Name:**************" + prName);
		IngestionLogDAO ingestionLogDAO = new IngestionLogDAO();
		String actionUserName = ingestionLogDAO.getRunlogInfo(prName);
		accessObject.addComponentExecution(prName, ZDPDaoConstant.PROJECT_ACTIVITY, actionUserName);
		ShellScriptExecutor exec = new ShellScriptExecutor();
		String[] args = new String[6];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home") + "/zeas/Config/CopyFilesFromLocalToHDFS.sh";
		args[2] = pipelinePath;
		args[3] = ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator;
		args[4] = pipelineName;
		args[5] = "/tmp/zeas/" + "p_" + pipelineName + File.separator + executionTime;
		System.out.println(args[0]);
		System.out.println(args[1]);

		System.out.println(args[2]);

		System.out.println(args[3]);

		System.out.println(args[4]);

		System.out.println(args[5]);

		ShellScriptExecutor.runScript(args);

		// Trigger Oozie workflow

		ProjectUtil.runOozieWorkflow(pipelinePath + "/job.properties", pipelineName, projectRunId, actions, prName,
				actionUserName);

		// Add Successfully Processed Pipeline to database
		// String outputDataSet =
		// stageList.get(stageList.size()-1).getOutputDataset();
		// DBUtility.saveProcessedPipeline(pipelineName,outputDataSet);

		System.out.println(actions);

	}

	/**
	 * Method gets pipeline details from DB and also gets associated stage
	 * details withing this pipeline.
	 * 
	 * @param pipelineName
	 *            {@link String} Name of the pipeline
	 * @return {@link Pipeline} object
	 */
	private Pipeline getPipelineDetails(String pipelineName) {

		Pipeline pipeline = new Pipeline();
		try {
			// Entity entity = DBUtility.getEntityDetails(pipelineName);

			Entity entity = DBUtility.getProjectDetails(pipelineName);
			if (entity != null) {
				String jsonBlob = entity.getJsonblob();
				JSONObject jObject = new JSONObject(jsonBlob);
				LOG.info("Processing for ====" + jObject.getString("name"));
				pipeline.setId((int) entity.getId());
				pipeline.setName(jObject.getString("name"));
				// pipeline.setFrequency(jObject.getString("frequency"));
				// pipeline.setOffset(jObject.getString("offset"));

				JSONObject stObj = (JSONObject) jObject.get("stageList");

				JSONArray stagesList = (JSONArray) stObj.get("stages");

				for (int i = 0; i < stagesList.length(); i++) {
					// Entity each =
					// DBUtility.getEntityDetails(stagesList.getJSONObject(i).getString("stageName"));
					Entity each = DBUtility
							.getTransformationDetails(stagesList.getJSONObject(i).getString("stageName"));
					if (each.getType() == null || each.getType().equalsIgnoreCase("dataset")
							|| each.getType().equalsIgnoreCase("Linear Regression")
							|| each.getType().equalsIgnoreCase("Binary Logistic Regression")
							|| each.getType().equalsIgnoreCase("MultiClass Logistic Regression")
							|| each.getType().equalsIgnoreCase("KMeans Clustering")
							|| each.getType().equalsIgnoreCase("Random Forest Regression")
							|| each.getType().equalsIgnoreCase("Random Forest Classification")) {
						continue;
					}
					Stage eachStage = this.getStageDetails(each, stagesList.getJSONObject(i), pipelineName);

					String parentStr = stagesList.getJSONObject(i).getString("input");
					eachStage.setInput(parentStr.split(","));
					String childStr;
					if (stagesList.getJSONObject(i).getString("output") == null) {
						childStr = "";
					} else {
						childStr = stagesList.getJSONObject(i).getString("output");
					}

					eachStage.setChild(childStr.split(","));
					// check for dataset
					String[] parentList = parentStr.split(",");
					StringBuilder tempList = new StringBuilder();
					for (String parent : parentList) {
						boolean isTransformation = true;
						String[] idVersion = parent.split("-");
						// read entity from entity table
						Entity tempEntity = DBUtility.getEntityById(Integer.parseInt(idVersion[0]));
						if (tempEntity != null && tempEntity.getId() == 0) {
							List<SearchCriterion> criterions = new ArrayList<>();
							SearchCriterion c1 = new SearchCriterion("id", idVersion[0], SearchCriteriaEnum.EQUALS);
							SearchCriterion c2 = new SearchCriterion("version", idVersion[1],
									SearchCriteriaEnum.EQUALS);
							criterions.add(c1);
							criterions.add(c2);
							// read from module table.
							ZDPDataAccessObjectImpl accessObject = new ZDPDataAccessObjectImpl();
							ProjectEntity temp = accessObject.findExactObject(ZDPDaoConstant.ZDP_MODULE_TABLE,
									criterions);
							if (temp.getName() != null && (temp.getName().equalsIgnoreCase("DATASET")
									|| temp.getName().equalsIgnoreCase("internal dataset")
									|| temp.getName().equalsIgnoreCase("Linear Regression")
									|| temp.getName().equalsIgnoreCase("Binary Logistic Regression")
									|| temp.getName().equalsIgnoreCase("MultiClass Logistic Regression")
									|| temp.getName().equalsIgnoreCase("KMeans Clustering")
									|| temp.getName().equalsIgnoreCase("Random Forest Regression")
									|| temp.getName().equalsIgnoreCase("Random Forest Classification"))) {
								isTransformation = false;
							}
							if (eachStage.getStageType().equalsIgnoreCase(ProjectConstant.TEST_MODEL_TRANSFORMATION)
									&& !temp.getName().equalsIgnoreCase(ProjectConstant.TRAIN_MODEL_TRANSFORMATION)) {
								isTransformation = false;
							}
						} else {
							isTransformation = false;
						}
						if (isTransformation) {
							if (tempList.length() > 0) {
								tempList.append(",");
							}
							tempList.append(parent);
						}
					}
					/*
					 * String s = tempList.toString().trim(); if(s.length()==0){
					 * eachStage.setParent(new String[2]); } else
					 */
					eachStage.setParent(tempList.toString().split(","));
					stages.add(eachStage);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Error fetching Pipeline details for " + pipeline);
			LOG.error(e.getMessage());
		}
		return pipeline;
	}

	/**
	 * Returns list of Forks for a given pipeline graph.
	 * 
	 * @param nodes
	 * @return
	 */
	private List<Stage> getForkList(List<Stage> nodes) {

		List<Stage> forklist = new ArrayList<>();
		for (Stage node : nodes) {
			// System.out.println("test");
			if (node.getParent() != null && node.getParent().length == 0) {
				forklist.add(node);
			}
		}
		return forklist;
	}

	/**
	 * Helper method which decides FORK/JOIN nodes for Oozie workflow
	 * definition. This has internal logic which check where should be fork and
	 * join various nodes to achieve paralled execution of complex workflow.
	 * 
	 * @param nodes
	 *            {@link List} of action nodes
	 * @return {@link List} of stages with fork and join properly defined.
	 */
	private List<Stage> getJoiningNodes(List<Stage> nodes) {

		List<Stage> tempNodes = new ArrayList<>();

		tempNodes.addAll(nodes);
		int indx = 0;
		List<String> startTo = new ArrayList<String>();
		List<Stage> endnodes = new ArrayList<>();
		for (Stage node : nodes) {

			String[] parents = node.getParent();
			String[] childs = node.getChild();
			if (parents != null && parents.length == 1) {
				for (String parent : parents) {
					String ok = node.getName();

					for (Stage temp : tempNodes) {
						if (temp.getName() != null) {
							if (temp.getName().equalsIgnoreCase(parent)) {
								if (temp.getOk_to() == null || !temp.getOk_to().contains("m_Fork")) {
									temp.setOk_to(ok);
								}
							}
						}
					}
				}
			} else if (parents != null && parents.length > 1) {
				Stage joinNode = new Stage();
				joinNode.setStageType(ProjectConstant.JOIN_NODE);
				joinNode.setOk_to(node.getName());
				String joinStageName = "joining";
				for (String parent : parents) {
					joinStageName += "_" + parent;
				}
				for (String parent : parents) {
					for (Stage temp : tempNodes) {
						if (temp.getName().equalsIgnoreCase(parent)) {
							temp.setOk_to(joinStageName);
						}
					}
				}
				joinNode.setName(joinStageName);
				tempNodes.add(indx, joinNode);
			}
			if (null != parents && parents[0].isEmpty()) {
				startTo.add(node.getName());
			}

			String[] children = node.getChild();
			// Flag to test if its a real fork node
			boolean isRealFork = true;
			if (children != null && children.length > 1) {
				// Incase Partition is used for Train/Test combination we don't
				// to run as Fork.
				if (node.getStageType().equals("Partition")) {
					for (Stage temp : tempNodes) {
						if (temp.getStageType().equals(ProjectConstant.TRAIN_MODEL_TRANSFORMATION)) {
							node.setOk_to(temp.getName());
							isRealFork = false;
							break;
						}
					}
				}
				if (isRealFork) {
					Stage forkNode = new Stage();

					forkNode.setStageType(ProjectConstant.FORK);
					forkNode.setChild(children);
					String forkName = "Fork";
					for (String child : children) {
						forkName += "_" + child;
					}
					forkNode.setName(forkName);
					tempNodes.add(indx + 1, forkNode);
					node.setOk_to(forkName);
					String[] parent = { node.getName() };
					forkNode.setParent(parent);
				}
			} else if (children != null && children.length == 0) {
				endnodes.add(node);
				node.setOk_to("end");
			}
			/*
			 * String ok = node.getOk_to().equalsIgnoreCase("end")?
			 * node.getOk_to():"m_"+node.getOk_to(); node.setOk_to(ok);
			 */
			indx++;
		}
		if (startTo.size() > 1) {
			Stage startingFork = new Stage();
			startingFork.setStageType(ProjectConstant.FORK);
			startingFork.setChild(startTo.toArray(new String[0]));
			String forkName = "Fork";
			for (String nodeName : startTo) {
				forkName += "_" + nodeName;
			}
			startingFork.setName(forkName);
			tempNodes.add(0, startingFork);
		}
		if (endnodes.size() > 1) {
			Stage joinNode = new Stage();
			joinNode.setStageType(ProjectConstant.JOIN_NODE);
			String joinStageName = "joining";
			for (Stage parent : endnodes) {
				joinStageName += "_" + parent.getName();
			}
			for (Stage parent : endnodes) {
				parent.setOk_to(joinStageName);
			}
			joinNode.setOk_to("end");
			joinNode.setName(joinStageName);
			tempNodes.add(indx + 1, joinNode);
		}
		return tempNodes;
	}

	/**
	 * Helper method populates stage details by parsing JSON from DB
	 * 
	 * @param entity
	 *            {@link Entity} object defining each stage
	 * @return Stage object
	 * @throws SQLException
	 */
	private Stage getStageDetails(Entity entity, JSONObject jsonObject, String projectName) throws Exception {
		Stage stage = new Stage();
		System.out.println("Parsing ==" + entity.getName());
		String jsonBlob = entity.getJsonblob();
		JSONObject sObject = new JSONObject(jsonBlob);
		try {
			// new implementation for proejct
			if (entity.getType().equalsIgnoreCase(ProjectConstant.COLUMN_SELECTOR_TRANSFORMATION)) {
				stage = TransformationUtil.getColumnSelectorStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.CLEAN_MISSING_DATA_TRANSFORMATION)) {
				stage = TransformationUtil.getColumnMissingDataStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.HIVE_TRANSFORMATION)) {
				stage = TransformationUtil.getHiveStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.PIG_TRANSFORMATION)) {
				stage = TransformationUtil.getPigStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.JOIN_TRANSFORMATION)) {
				stage = TransformationUtil.getJOINStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.PARTITION_TRANSFORMATION)) {
				stage = TransformationUtil.getPartitionStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.SUBSET_TRANSFORMATION)) {
				stage = TransformationUtil.getSubSetStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.TRAIN_MODEL_TRANSFORMATION)) {
				stage = TransformationUtil.getTrainModelStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.MAPRED_TRANSFORMATION)) {
				stage = TransformationUtil.getMapRedStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.GROUP_BY_TRANSFORMATION)) {
				stage = TransformationUtil.getGroupByStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.TEST_MODEL_TRANSFORMATION)) {
				stage = TransformationUtil.getTestModelStage(entity, jsonObject, projectName, executionTime);
			} else if (entity.getType().equalsIgnoreCase(ProjectConstant.COMPARE_MODEL_TRANSFORMATION)) {
				stage = TransformationUtil.getCompareModelStage(entity, jsonObject, projectName, executionTime);
			}

			// end
			if (entity.getType().equals("PipelineStage")) {
				stage.setName(sObject.getString("stagename"));
				stage.setStageType(sObject.getString("seletType"));
				stage.setOutputDataset(sObject.getString("outDataset"));
				Entity outDataset = DBUtility.getEntityDetails(stage.getOutputDataset());

				String datasetBlob = outDataset.getJsonblob();
				JSONObject dataset = new JSONObject(datasetBlob);
				stage.setOutputPath(dataset.getString("location"));

				Entity inputDS = DBUtility.getEntityDetails(sObject.getString("inputDataset"));

				String input = inputDS.getJsonblob();
				JSONObject inDS = new JSONObject(input);
				stage.setInputPath(inDS.getString("location"));

				if (sObject.getString("seletType").equals("Pig")) {
					stage.setPigScript(sObject.getString("pig"));
					stage.setOutputSchema(dataset.getString("Schema"));
					// Entity outSchema =
					// DBUtility.getEntityDetails(dataset.getString("Schema"));
				}
				if (sObject.getString("seletType").equals("Hive")) {
					stage.setHiveScript(sObject.getString("hiveSql"));
				}
				if (sObject.getString("seletType").equals("MapReduce")) {
					stage.setMapperClass(sObject.getString("mapclass"));
					stage.setReducerClass(sObject.getString("reduceclass"));
					stage.setMapRedJarPath(sObject.getString("mapRedJarPath"));
					if (sObject.getString("outKeyClass") != null || sObject.getString("outValueClass") != null) {
						stage.setOutKeyClassName(sObject.getString("outKeyClass"));
						stage.setOutValueClassName(sObject.getString("outValueClass"));
					} else {
						stage.setOutKeyClassName("");
						stage.setOutValueClassName("");
					}

				}
			}

		} catch (JSONException e) {
			LOG.error(e.getMessage());
		}
		return stage;
	}

	/**
	 * Helper method generates Hive action XML for Oozie workflow definition.
	 * 
	 * @param stage
	 *            {@link Stage} for Hive action
	 * @param pipeline
	 *            {@link String} name of the pipeline
	 * @return Hive action XML element
	 */
	/*
	 * private Element addHiveAction(Stage stage, String pipeline, String
	 * additionalQuery) { WorkflowBuilder builder = new WorkflowBuilder();
	 * String hqlPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") +
	 * File.separator + pipeline + File.separator + stage.getName() + ".hql";
	 * String hdfsAppPath =
	 * ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") +
	 * File.separator + pipeline + File.separator; builder.writeToFile(
	 * PipelineUtil.getHiveQuery(stage.getHiveScript(),
	 * stage.getAbsTransformation().getOutputLocation(), "m_" +
	 * stage.getName().replace("-", "_"), additionalQuery), hqlPath); return
	 * builder.getHiveActionTemplate("m_" + stage.getName(), stage.getOk_to(),
	 * hdfsAppPath + stage.getName() + ".hql"); }
	 */
	
	
	
	private Element addHiveAction(Stage stage, String pipeline, String additionalQuery) {
		WorkflowBuilder builder = new WorkflowBuilder();
		String hqlPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".sh";
		String hdfsAppPath = ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline
				+ File.separator;
		System.out.println("stage.getHiveScript()===" + stage.getHiveScript());
		builder.writeToFile(
				PipelineUtil.getHiveQuery(stage.getHiveScript(), stage.getAbsTransformation().getOutputLocation(),
						"m_" + stage.getName().replace("-", "_"), additionalQuery),
				hqlPath);
		return builder.getHiveActionTemplate("m_" + stage.getName(), stage.getOk_to(),
				hdfsAppPath + stage.getName() + ".sh");
	}
	


	/**
	 * Helper method generates PIG action XML for Oozie workflow definition.
	 * 
	 * @param stage
	 *            stage {@link Stage} for PIG action
	 * @param pipeline
	 *            pipeline {@link String} name of the pipeline
	 * @return PIG action XML element
	 */
	private Element addPigAction(Stage stage, String pipeline, Map<String, AbstractTransformation> inputTablesToLoad) {
		WorkflowBuilder builder = new WorkflowBuilder();

		String pigScriptPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".pig";

		String pigHDFSAppPath = ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline
				+ File.separator;
		String pigUDFPath = "";

		PigScriptTransformation pt = (PigScriptTransformation) stage.getAbsTransformation();
		/**
		 * Logic to check if this Pig Action uses any custom UDF, if so register
		 * those UDFs here.
		 */
		if (pt.getPigUdfJarPath() != null && !pt.getPigUdfJarPath().isEmpty()) {
			String udfJarName = pt.getPigUdfJarPath().substring(pt.getPigUdfJarPath().lastIndexOf("/") + 1);
			String hdfsJarPath = ConfigurationReader.getProperty("HDFS_FQDN") + File.separator + pigHDFSAppPath + "lib"
					+ File.separator + udfJarName;
			pigUDFPath = "REGISTER " + hdfsJarPath + ";\n";
			// copy the udf jar file to pipeline workflow directory.
			PipelineUtil.copyJarFile(ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline,
					pt.getPigUdfJarPath());
		}
		builder.writeToFile(
				PipelineUtil.getPigScript(stage.getPigScript(), stage.getAbsTransformation().getOutputLocation(),
						"m_" + stage.getName().replace("-", "_"), inputTablesToLoad, pigUDFPath),
				pigScriptPath);
		return builder.getPigActionTemplate("m_" + stage.getName(), stage.getOk_to(),
				pigHDFSAppPath + stage.getName() + ".pig", stage.getInputPath(), stage.getOutputPath());
	}

	/**
	 * Helper method generates Shell action XML for Oozie workflow definition.
	 * 
	 * @param stage
	 *            {@link Stage} for Shell action
	 * @param pipeline
	 *            pipeline {@link String} name of the pipeline
	 * @return Shell action XML element
	 */
	private Element addShellAction(Stage stage, String pipeline) throws Exception {
		WorkflowBuilder builder = new WorkflowBuilder();
		String shellScriptPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".sh";
		String hiveScript = REG_TABLE_SCRIPT.replace("$1", stage.getOutputDataset());
		hiveScript = hiveScript.replace("$2", getTableSchema(stage.getOutputSchema()));
		hiveScript = hiveScript.replace("$3", stage.getOutputPath());
		builder.writeToFile(hiveScript, shellScriptPath);
		return builder.getShellActionTemplate(stage.getName(), stage.getOk_to(),
				ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline + File.separator
						+ stage.getName() + ".sh");
	}

	/**
	 * Helper method generates MapReduce action XML for Oozie workflow
	 * definition.
	 * 
	 * @param stage
	 *            stage {@link Stage} for MapReduce action
	 * @param pipeline
	 *            pipeline {@link String} name of the pipeline
	 * @return MapReduce action XML element
	 */
	private Element addMapRedAction(Stage stage, String pipeline) {
		WorkflowBuilder builder = new WorkflowBuilder();
		MapReduceTransformation mrTrnf = (MapReduceTransformation) stage.absTransformation;
		// copy the jar file to pipeline workflow directory.
		PipelineUtil.copyJarFile(ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline,
				mrTrnf.getMapredJarPath());
		// set mapper class and reducer class name
		return builder.getMRActionTemplate("m_" + stage.getName(), stage.getOk_to(), mrTrnf.getMapperClass(),
				mrTrnf.getReducerClass(), mrTrnf.getInputLocation(), mrTrnf.getOutputLocation(),
				mrTrnf.getOutputKeyClass(), mrTrnf.getOutputValueClass());
	}

	/**
	 * Generates schema definition for Hive Table given schema name
	 * 
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 */
	private String getTableSchema(String schemaName) throws Exception {

		String schemaStr = DBUtility.getJSON_DATA(schemaName);
		DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
		DataSchema schema = parser.getDSConfigDetails(schemaStr);
		return HiveClient.getSchemaAttributes(schema.getDataAttribute()).toString();
	}

	public static void main(String[] args) {

		/*String str = "/user/zeas/admin/CsvTesting3//cleansed;/tmp/zeas/p_3668-4/1496402995725/3669-1;0,1,2;Custom_Value;hello";
		String str1 = str.substring(str.indexOf(';') + 1);
		String str2 = str1.substring(str1.indexOf(';') + 1);
		String schema = str2.substring(0, str2.indexOf(';'));
		String str3 = str2.substring(str2.indexOf(';') + 1);
		String[] selCol = str3.split(",");
		for (String col : selCol) {
			schema = schema + "," + col.substring(0, col.indexOf(':'));
		}
		System.out.println(schema);*/
		
		String tablename="zeas.m_3702_1_split2";
		System.out.println(tablename.substring(0, tablename.lastIndexOf("_")));

	}
// transformation ation for lineage
	/*private Element addtransformationAction(Stage stage, String pipeline) throws Exception {
		String type = stage.absTransformation.getType();
		WorkflowBuilder builder = new WorkflowBuilder();
		String previousTableName;
		String schema = null;
		String shellScriptPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".sh";
		if (type.equalsIgnoreCase("Join")) {
			builder.writeToFile(TRANSFORM_SUBMIT_COMMAND, shellScriptPath);
		} else {

			String tableName = stage.getName().replace('-', '_');
			String str = stage.absTransformation.getInputArgsString();
			String datasetName = str.substring(0, str.indexOf(';'));

			if (datasetName.endsWith("cleansed")) {
				datasetName = datasetName.substring(0, datasetName.lastIndexOf('/'));
				if (datasetName.endsWith("/")) {
					datasetName = datasetName.substring(0, datasetName.lastIndexOf('/'));
					previousTableName = datasetName.substring(datasetName.lastIndexOf('/') + 1);
					previousTableName = "zeas." + previousTableName + "_DataSet";
				} else {
					previousTableName = datasetName.substring(datasetName.lastIndexOf('/'));
					previousTableName = "zeas." + previousTableName + "_DataSet";

				}
			} else {
				previousTableName = datasetName.substring(datasetName.lastIndexOf('/') + 1);
				previousTableName = previousTableName.replace('-', '_');
				previousTableName = "zeas.m_" + previousTableName;
			}
			if(previousTableName.endsWith("split1") || previousTableName.endsWith("split2")){
				previousTableName=previousTableName.substring(0, previousTableName.lastIndexOf("_"));
			}
			if (type.equalsIgnoreCase("Column Filter")) {
				String str1 = str.substring(str.indexOf(';') + 1);
				int index = str1.indexOf(';') + 1;
				String pathLocation = str1.substring(0, str1.indexOf(';'));
				String selectedColumns = str1.substring(index);
				String[] selCol = selectedColumns.split(",");
				System.out.println("selCol===" + selCol);
				String arg = stage.absTransformation.getInputDatasetSchema();
				String[] column = arg.split(",");
				StringBuilder finalSelectedColumns = new StringBuilder();
				for (String sc : selCol) {
					String a[] = column[Integer.parseInt(sc)].split(":");
					finalSelectedColumns.append(a[0]).append(",");
				}
				finalSelectedColumns.setLength(finalSelectedColumns.length() - 1);
				schema = finalSelectedColumns.toString();
			}
			if (type.equalsIgnoreCase("Group By")) {
				String str1 = str.substring(str.indexOf(';') + 1);
				String str2 = str1.substring(str1.indexOf(';') + 1);
				schema = str2.substring(0, str2.indexOf(';'));
				String str3 = str2.substring(str2.indexOf(';') + 1);
				String[] selCol = str3.split(",");
				for (String col : selCol) {
					schema = schema + "," + col.substring(0, col.indexOf(':'));
				}
				System.out.println(schema);
			}
		
			if (type.equalsIgnoreCase("Clean Missing Data")) {
				String str1 = str.substring(str.indexOf(';') + 1);
				String str2 = str1.substring(str1.indexOf(';') + 1);
				String selectedColumns = str2.substring(0, str2.indexOf(';'));
				String[] selCol = selectedColumns.split(",");
				String arg = stage.absTransformation.getInputDatasetSchema();
				String[] column = arg.split(",");
				StringBuilder finalSelectedColumns = new StringBuilder();
				for (String sc : selCol) {
					String a[] = column[Integer.parseInt(sc)].split(":");
					finalSelectedColumns.append(a[0]).append(",");
				}
				finalSelectedColumns.setLength(finalSelectedColumns.length() - 1);
				schema = finalSelectedColumns.toString();
			}
			String hiveScript = REG_TABLE_SCRIPT.replace("$4", tableName);
			if (type.equalsIgnoreCase("Column Filter") || type.equalsIgnoreCase("Clean Missing Data") || type.equalsIgnoreCase("Group By")) {
				hiveScript = hiveScript.replace("$5", schema);
			} else {
				hiveScript = hiveScript.replace("$5", "*");
			}
			hiveScript = hiveScript.replace("$6", previousTableName);

			StringBuilder sb = new StringBuilder();
			sb.append(TRANSFORM_SUBMIT_COMMAND).append('\n').append(hiveScript);
			builder.writeToFile(sb.toString(), shellScriptPath);
		}

		return builder.getTransformActionTemplate("m_" + stage.getName(), stage.getOk_to(),
				ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline + File.separator
						+ stage.getName() + ".sh",
				stage.absTransformation.getType(), stage.absTransformation.getInputDatasetSchema(),
				stage.absTransformation.getInputArgsString(), ProjectConstant.TRANSFORMATION_JAR_PATH);

	}*/
	
	private Element addtransformationAction(Stage stage, String pipeline) throws SQLException {
		WorkflowBuilder builder = new WorkflowBuilder();
		String shellScriptPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".sh";

		builder.writeToFile(TRANSFORM_SUBMIT_COMMAND, shellScriptPath);
		return builder.getTransformActionTemplate("m_" + stage.getName(), stage.getOk_to(),
				ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline + File.separator
						+ stage.getName() + ".sh",
				stage.absTransformation.getType(), stage.absTransformation.getInputDatasetSchema(),
				stage.absTransformation.getInputArgsString(), ProjectConstant.TRANSFORMATION_JAR_PATH);
	}

	private Element addMachineLearningAction(Stage stage, String pipeline) throws SQLException {
		WorkflowBuilder builder = new WorkflowBuilder();
		String shellScriptPath = ConfigurationReader.getProperty("PIPELINE_APP_DATA") + File.separator + pipeline
				+ File.separator + stage.getName() + ".sh";

		builder.writeToFile(ML_SUBMIT_COMMAND, shellScriptPath);
		return builder.getTransformActionTemplate("m_" + stage.getName(), stage.getOk_to(),
				ConfigurationReader.getProperty("PIPELINE_APP_HDFS_DATA") + File.separator + pipeline + File.separator
						+ stage.getName() + ".sh",
				stage.absTransformation.getType(), stage.absTransformation.getInputDatasetSchema(),
				stage.absTransformation.getInputArgsString(), ProjectConstant.ML_LIB_JAR_PATH);
	}

	private Element getForkNode(Stage stage) {
		Element fork = new Element(ProjectConstant.FORK);
		fork.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		fork.setAttribute("name", "m_" + stage.getName());

		for (int i = 0; i < stage.getChild().length; i++) {
			Element path = new Element(ProjectConstant.PATH);
			path.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
			String child = stage.getChild()[i];
			path.setAttribute(ProjectConstant.START, "m_" + child);
			fork.addContent(path.detach());
		}

		return fork;
		// fork.addContent(paths);
		// return fork.addContent(paths);
	}

	private Element getJoinNode(Stage stage) {
		Element join = new Element(ProjectConstant.JOIN);
		join.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		join.setAttribute("name", "m_" + stage.getName());
		join.setAttribute("to", stage.getOk_to());
		/*
		 * Element ok_to = new Element(ProjectConstant.OK);
		 * ok_to.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		 * ok_to.setAttribute("to", stage.getOk_to()); join.addContent(ok_to);
		 */
		return join;
	}

}
