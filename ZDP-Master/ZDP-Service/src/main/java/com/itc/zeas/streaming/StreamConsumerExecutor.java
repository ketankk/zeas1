package com.itc.zeas.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.streaming.daoimpl.StreamDriverManager;
import com.itc.zeas.streaming.model.StreamingEntity;
import com.itc.zeas.utility.utility.ConfigurationReader;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.taphius.databridge.deserializer.DataSourcerConfigDetails;
import com.taphius.databridge.model.DataSchema;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.pipeline.HiveClient;
import com.zdp.dao.ZDPDataAccessObjectImpl;

public class StreamConsumerExecutor implements StreamExecutor {

	private static final String SPARK = "sparkC";
	public static Logger LOG = Logger.getLogger(StreamConsumerExecutor.class);

	public String executeConsumer(Entity entity) {
		String json = entity.getJsonblob();
		LOG.info("Starting Consumer name: " + entity.getName());

		JSONObject jsonObject = new JSONObject(json);

		return startConsumer(jsonObject, entity.getName(), entity.getCreatedBy());

	}

	public static void main(String[] args) throws Exception {
		StreamingEntity entity = new StreamingEntity();
		entity.setName("ZeasDemoCon12");
		entity.setType("Consumer");
		new StreamConsumerExecutor().startStream(entity);
	}

	public String startConsumer(JSONObject json, String consumer, String user) {
		try {
			ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();

			accessObjectImpl.addComponentExecution(consumer, ZDPDaoConstant.STREAM_ACTIVITY, user);

			System.out.println("Reading Json from String....");
			String type = json.get("type").toString();// spark or flink
			String hostname = json.get("Hostname").toString();
			String port = json.get("Port").toString();
			String groupID = json.get("Group_ID").toString();
			String output = json.get("Output_Location").toString();
			String topic = json.getJSONObject("Topic").getString("topicName");
			String ruleName = json.get("ruleName").toString();

			// these 3 are for transformation plugin
			String jarLocation = null;
			String fqcn = null;
			String methodName = null;
			// If user has selected any rule then use transformed data from
			// topic
			if (ruleName != null) {
				List<String> ruleDet = getRuleDetail(ruleName);
				if (ruleDet != null) {
					jarLocation = ruleDet.get(0);
					fqcn = ruleDet.get(1);
					methodName = ruleDet.get(2);
				}
			}
			String duration = null;
			// Duration poll time is required only for spark
			if (type.equalsIgnoreCase(SPARK))
				duration = json.get("duration").toString();

			if (output.charAt(output.length() - 1) != '/') {
				output = output + "/";
			}
			String[] args = new String[16];
			args[0] = ShellScriptExecutor.BASH;
			args[1] = System.getProperty("user.home") + "/zeas/Config/Streaming/Consumer-Start.sh";
			args[2] = type;
			args[3] = hostname;
			args[4] = port;
			args[5] = groupID;
			args[6] = topic;
			args[7] = user;
			args[8] = duration;
			args[9] = consumer;
			if (jarLocation != null && fqcn != null && methodName != null) {
				args[10]="--jar";
				args[11] = jarLocation;
				args[12]="--fqcn";
				args[13] = fqcn;
				args[14]="--tranmthd";
				args[15] = methodName;
			}
			args=clean(args);

			// TODO add this consumer name in script file
			LOG.info("Arguments set for Consumer-start.sh " + Arrays.toString(args));
			ShellScriptExecutor.runScript(args);
			
			/*ProcessBuilder pb = new ProcessBuilder(args);
			// Redirect the errorstream
			pb.redirectErrorStream(true);
			Process p = null;

			p = pb.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String text;
			String driver = null;
			LOG.info("Messages from Consumer Start Process builder ...");
			while ((text = br.readLine()) != null) {
				LOG.info(text);
			//	System.out.println("iNSIDE pb " + text);
				
				 * if (text.contains("Submitted application")) { int start =
				 * text.lastIndexOf("_") - 25; //int end = start + 30; driver =
				 * text.substring(start); LOG.info("Application Id "+driver);
				 * break; }
				 
			}
*/
			LOG.info("Consumer is consuming...................");


		} catch (Exception e) {
			LOG.error("Error: " + e.getMessage());
			return "Failed";
		}

		return "Success";
	}
	//method to remove null values from array
	 private static String[] clean(final String[] v) {
		    List<String> list = new ArrayList<String>(Arrays.asList(v));
		    list.removeAll(Collections.singleton(null));
		    return list.toArray(new String[list.size()]);
		}
	private List<String> getRuleDetail(String ruleName) throws SQLException {
		// TODO Auto-generated method stub

		StreamDriverManager driverManager = new StreamDriverManager();
		List<String> ruleDet = driverManager.getRuleDetailsByName(ruleName);
		
		LOG.info("Rule details for "+ruleName+" are "+ruleDet);
		return ruleDet;

	}

	public void stopConsumer(String consumer, String driverId, String user) {
		// final String stopScript="/home/zeasStream/Consumer-Stop.sh";
		ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();
		String[] args = new String[3];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home") + "/zeas/Config/Consumer-Stop.sh";
		args[2] = driverId;
		ProcessBuilder pb = new ProcessBuilder(args);
		// Redirect the errorstream
		pb.redirectErrorStream(true);
		pb.redirectErrorStream(true);
		Process p = null;
		BufferedReader br = null;
		try {

			p = pb.start();
			/*
			 * Session session=createSSHSession(); ChannelExec channelExec =
			 * (ChannelExec)session.openChannel("exec"); channelExec.setCommand(
			 * "sh "+stopScript+" "+driverId); channelExec.connect(); int
			 * exitStatus = channelExec.getExitStatus(); System.out.println(
			 * "sh "+stopScript+" "+driverId); BufferedReader br=new
			 * BufferedReader(new
			 * InputStreamReader(channelExec.getInputStream())); String text;
			 * 
			 * while((text=br.readLine())!=null){ System.out.println(text); }
			 */
			br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String text;
			while ((text = br.readLine()) != null) {
				if (text.contains("Killed application " + driverId)) {
					// Thread.sleep(2000);
					break;
				}
			}

			LOG.info("Consumer stopped succesfully.....................................");

			accessObjectImpl.addComponentRunStatus(consumer, ZDPDaoConstant.STREAM_ACTIVITY,
					ZDPDaoConstant.JOB_TERMINATE, driverId, user);

		} catch (Exception e) {
			System.err.println("Error: " + e);
			LOG.error("Error: " + e);

		} finally {
			// close bufferedReader connection
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	@SuppressWarnings("unused")
	private Session createSSHSession() {

		final String sparkHost = ConfigurationReader.getProperty("SPARK_HOSTNAME");
		final String sshPort = ConfigurationReader.getProperty("SSH_PORT");
		final String sparkUser = ConfigurationReader.getProperty("SPARK_USER");
		final String sparkPassword = ConfigurationReader.getProperty("SPARK_PASSWORD");
		Properties prop = new Properties();
		prop.setProperty("StrictHostKeyChecking", "no");
		JSch jsch = new JSch();
		Session session = null;

		try {
			session = jsch.getSession(sparkUser, sparkHost, Integer.parseInt(sshPort));
			session.setConfig(prop);
			session.setPassword(sparkPassword);
			session.connect();
			return session;
		} catch (NumberFormatException e) {
			System.err.println("Error: " + e);
			LOG.error("Error: " + e);
		} catch (JSchException e) {
			System.err.println("Error: " + e);
			LOG.error("Error: " + e);
		} catch (Exception e) {
			System.err.println("Error: " + e);
			LOG.error("Error: " + e);
		}
		return null;
	}

	@Override
	public String startStream(StreamingEntity entity) throws Exception {

		StreamingEntity streamingEntity = null;
		try {
			String entityName = entity.getName();

			StreamDriverManager manager = new StreamDriverManager();
			streamingEntity = manager.getEntityByName(entityName);
		} catch (SQLException | IOException e) {
		}
		String json = streamingEntity.getJsonblob();
		String conName = streamingEntity.getName();
		JSONObject jsonObject = new JSONObject(json);
		String location = jsonObject.get("Output_Location").toString();
		DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
		try {
			LOG.info("Creating hive table for " + streamingEntity.getName());
			DataSchema schema = parser.getDSConfigDetails(streamingEntity.getSchemaJson());
			String streamSchema = HiveClient.getSchemaAttributes(schema.getDataAttribute()).toString();
			System.out.println("Schema : " + streamSchema);
			LOG.info("Schema for hive table is " + streamSchema);
			String[] args = new String[5];
			args[0] = ShellScriptExecutor.BASH;
			args[1] = System.getProperty("user.home") + "/zeas/Config/Streaming/createHiveTableStreaming.sh";
			args[2] = jsonObject.getString("datasetName");
			args[3] = streamSchema;
			args[4] = location + "/" + conName;
			ShellScriptExecutor.runScript(args);
		} catch (Exception e) {
			LOG.error("Schema not correct for hive table..can't create hive table " + e.getMessage());
			// throw new ZeasException(9506,"Schema not correct for hive table
			// "+e.getMessage(),"");
		}

		String res = startConsumer(new JSONObject(streamingEntity.getJsonblob()), streamingEntity.getName(),
				streamingEntity.getCreatedBy());
		return res;

	}

	@Override
	public boolean stopStream(StreamingEntity entity) {
		// TODO Auto-generated method stub
		return false;
	}

}
