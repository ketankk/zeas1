package com.itc.zeas.streaming;

import java.util.Arrays;

import com.google.gson.JsonParser;
import com.itc.zeas.dashboard.dashboard.ProcessMonitorService;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import org.apache.log4j.Logger;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.itc.zeas.streaming.model.KafkaDetails;
import com.itc.zeas.streaming.model.StreamingEntity;
import org.springframework.http.ResponseEntity;

public class StreamTransExecutor implements StreamExecutor {

	private static final Logger LOG = Logger.getLogger(StreamTransExecutor.class);

	@Override
	public String startStream(StreamingEntity entity) throws Exception {

		KafkaHelper helper = new KafkaHelper();
		KafkaDetails details = helper.kafkaDetails();
		try {

			// System.out.println("Reading Json from String....");
			LOG.info("Starting Transformation consumer " + entity.getName());
			String topicCon = entity.getJsonBlob().get("Topic").get("topicName").toString();
			// All the transformed data using transformation rule
			// 'entity.getName()' from topic topicCon will go into new topic
			// topic_prod
			String topicProd = topicCon.concat("_" + entity.getName());
			//"" double quotes are coming from UI, that is bnot allowed as topic name
			topicCon = topicCon.replaceAll("\"", "");
			topicProd = topicProd.replaceAll("\"", "");

			if (!KafkaHelper.validateTopic(topicCon) || !KafkaHelper.validateTopic(topicProd)) {
				LOG.error("Topic name can't contain special character");
				throw new ZeasException(ZeasErrorCode.NOT_VALID_STRING, "Topic name can't contain special character",
						"Topic name can't contain special character  use only alphanumerics, '.', '_' and '-'");
			}
			String jarLoc = entity.getJsonBlob().get("Output_Location").toString().replaceAll("\"", "");
			String[] args = new String[10];
			args[0] = ShellScriptExecutor.BASH;
			args[1] = System.getProperty("user.home") + "/zeas/Config/Streaming/TransformationConsumer-Start.sh";
			args[2] = jarLoc;// jar
			// location
			// FQN
			args[3] = details.getZkhostName();
			args[4] = String.valueOf(details.getZkhostPort());
			args[5] = topicCon + "_" + topicProd;// kafka consumer group
			args[6] = topicCon;
			args[7] = entity.getCreatedBy();
			args[8] = "5";// harcoded poll time for spark consumer
			args[9] = topicProd;
			// TODO add this consumer name in script file
			LOG.info("Arguments set for Consumer-start.sh " + Arrays.toString(args));
			ShellScriptExecutor.runScript(args);

			LOG.info("TrasnformConsumer is consuming from " + topicCon + " and writing to " + topicProd);

		} catch (Exception e) {
			LOG.error("Couldn't execute TransformationConsumer-Start.shscript Error: " + e.getMessage());
			return "Failed";
		}

		return "Success";
	}

	@Override
	public boolean stopStream(StreamingEntity entity) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean addRule(StreamingEntity entity) throws Exception {
		LOG.info("Adding new rule " + entity.toString());
		TransformationRule trans = new TransformationRule();
		// first check if rule exist
		if (trans.getRulesName().contains(entity.getName())) {
			LOG.info("Rule with name '" + entity.getName() + "' already exists");
			throw new ZeasException(9505, "Rule already exist ", "Rule already exist " + entity.getName());
		}

		
		return trans.addRule(entity);

	}
	public static void main(String[] args) throws Exception {
		StreamingEntity entity=new StreamingEntity();
		entity.setName("Nameee");
		new StreamTransExecutor().addRule(entity);
		JsonParser parser=new JsonParser();
		//parser.parse(arg0)
		//JsonNode jsonBlob=;
		//entity.setJsonBlob(jsonBlob);
	}
	/**
	 * move this method to some other class in service .keeping here for now
	 * @param name
	 * @param userName
	 * @return
	 * @throws Exception
	 */

	public ResponseEntity<Object> getStreamDriver(String name, String userName) throws Exception {

		ProcessMonitorService monitorService = new ProcessMonitorService();
		return monitorService.getRunningProcesses(userName, name, ZDPDaoConstant.STREAM_ACTIVITY, true);
	}
}
