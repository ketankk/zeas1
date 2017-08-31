package com.itc.zeas.streaming;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.streaming.model.KafkaDetails;
import com.itc.zeas.streaming.model.StreamingEntity;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class StreamProducerExecutor implements StreamExecutor {

	Logger LOG = Logger.getLogger(getClass());
	private static final String STREAMING_HOME = System.getProperty("user.home") + "/zeas/Config/Streaming/";
	private static final String FLUME_CONF_HOME = STREAMING_HOME + "flumeconf/";

	private static final String FLUME_HOME = ConfigurationReader.getProperty("FLUME_HOME");
	private static final String FLUME = "flumeP";
	private static final String TWITTER = "twitterP";
	ShellScriptExecutor executor = new ShellScriptExecutor();

	@Override
	public String startStream(StreamingEntity entity) throws Exception {
		// check type twitter or flume
		String jsonBlob = entity.getJsonblob();

		JSONObject jsonObject = new JSONObject(jsonBlob);

		String prodType = jsonObject.getString("type");
		String producerDetail = jsonObject.getString("producerDetail");
		String kafkaTopic = jsonObject.getJSONObject("Topic").getString("topicName");

		LOG.info("Starting Poducer stream for Producer type:" + prodType);
		switch (prodType) {
		case FLUME:
			runFlumeProducer(producerDetail, entity.getName());
			break;
		case TWITTER:
			runTwitterProducer(producerDetail, kafkaTopic, entity.getName());
			break;
		}

		// run shell script

		return "Success";
	}

	@Override
	public boolean stopStream(StreamingEntity entity) {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean runFlumeProducer(String producerDetail, String producerName) throws ZeasException {
		String confFilePath = null;
		JSONObject jsonObject = new JSONObject(producerDetail);

		String flumeConfText = jsonObject.getString("flumeText");
		String agentName = jsonObject.getString("agentName");

		try {
			LOG.info("Creating conf file");
			confFilePath = generateFlumeConfFile(flumeConfText, producerName);
		} catch (Exception e) {
			LOG.error("Couldn't create conf file for flume " + e.getMessage());
			//throw new ZeasException(9555,"Couldn't create conf file for flume","");
		}

		String args[] = new String[5];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = STREAMING_HOME + "startFlumeProducer.sh";
		args[2] = FLUME_HOME;
		args[3] = agentName;
		args[4] = confFilePath;

		LOG.info("Executing startFlumeProducer.sh with arguments " + Arrays.toString(args));
		if (executor.runScript(args) == 0)
			return true;

		return false;
	}

	/**
	 * Method takes string for flume conf file and creates a file 
	 * 
	 * @param flumeConfText
	 * @param prodcerName+".conf"
	 * @return
	 * @throws Exception
	 */
	private String generateFlumeConfFile(String flumeConfText, String producerName) throws Exception {

		String pathname = FLUME_CONF_HOME + producerName + ".conf";
		LOG.info("Conf file can be found at this location "+pathname);

		Files.write(Paths.get(pathname), flumeConfText.getBytes());
		return pathname;
	}

	private boolean runTwitterProducer(String jsonData, String kafkaTopic, String producerName) throws ZeasException {

		String consumerKey = "K4ayStey37fRceUBKXSqE2XGN";
		String consumerSecret = "JZtkftUhSLQEsD9RrzDgMfr06qvrBMMta0O2TalIYA6997El0s";
		String token = "156187317-ZY8hyuXxt9ADAAd6jAMcKuCHd9jrVy2T66n0c3DG";
		String secret = "nMLQlEA0X3GbDzZbQYNrM10KUuRV889APhptJJJqvSrKC";

		JSONObject jsonObject = new JSONObject(jsonData);

		consumerKey = jsonObject.getString("consumerKey");
		consumerSecret = jsonObject.getString("consumerSecret");
		token = jsonObject.getString("token");
		secret = jsonObject.getString("secret");
		/**
		 * kafkahelper to get default kafka installed on system
		 */
		KafkaHelper helper = new KafkaHelper();
		KafkaDetails kafkaDetails = helper.kafkaDetails(null);

		String args[] = new String[9];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = STREAMING_HOME + "startTwitterProducer.sh";
		args[2] = consumerKey;
		args[3] = consumerSecret;
		args[4] = token;
		args[5] = secret;
		args[6] = String.valueOf(kafkaDetails.getKafkabrokerlist().get(0));
		args[7] = kafkaTopic;
		LOG.info("Executing startTwitterProducer.sh with arguments " + Arrays.toString(args));

		executor.runScript(args);

		return true;
	}

}
