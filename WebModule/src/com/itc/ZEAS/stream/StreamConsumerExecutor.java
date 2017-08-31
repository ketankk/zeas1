package com.itc.zeas.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.itc.taphius.dao.StreamDriverManager;
import com.itc.taphius.model.Entity;
import com.itc.taphius.utility.ConfigurationReader;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;


public class StreamConsumerExecutor {

	public static Logger LOG = Logger.getLogger(StreamConsumerExecutor.class);
	//private Session session;

	public String executeConsumer(Entity entity){
		String json=entity.getJsonblob();
		System.out.println("Starting Consumer name: "+entity.getName());
		return startConsumer(new JSONObject(json),entity.getName(),entity.getCreatedBy());

	} 

	public String startConsumer(JSONObject json,String consumer,String user){
		final String startScript=ConfigurationReader.getProperty("CONSUMER_START_SCRIPT");
		//final String startScript="/home/19491/Consumer-Start.sh";
		System.out.println("Reading Json from String....");

		String hostname=json.get("Hostname").toString();
		String port=json.get("Port").toString();
		String groupID=json.get("Group_ID").toString();
		final String output=json.get("Output_Location").toString();
		String topic=json.get("Topic").toString();
		String duration=json.get("duration").toString();

		String arguments=hostname+" "+port+" "+groupID+" "+topic+" "+output +" "+duration;

		System.out.println(hostname+"\t"+port+"\t"+groupID+"\t"+topic+"\t"+output+"\t"+duration);
		LOG.info(hostname+"\t"+port+"\t"+groupID+"\t"+topic+"\t"+output+"\t"+duration);


		try{
			Session session=createSSHSession();
			ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
			channelExec.setCommand("sh "+startScript+" "+arguments);
			channelExec.connect();
			int exitStatus = channelExec.getExitStatus();
			
			/*
			Reader reader = new InputStreamReader(channelExec.getInputStream());

		    char[] buf = new char[1024];
		    int numRead;
		    while ((numRead = reader.read(buf)) != -1) {
		        String readData = String.valueOf(buf, 0, numRead);
		        System.out.print(readData);
		        buf = new char[1024];
		    }
		    
		    */
			
			BufferedReader br=new BufferedReader(new InputStreamReader(channelExec.getInputStream()));
			String text;
			String driver=null;
			while((text=br.readLine())!=null){
				System.out.println(text);
				if(text.startsWith("Driver")){
					driver=text.substring(text.lastIndexOf(" ")+1);
					break;
				}
					
			}
			System.out.println("DRIVER-------------:"+driver);
			
						
			System.out.println("Consumer streamer is running.....................................");
			LOG.info("Consumer streamer is running.....................................");

			/*
			 * mapping consumer and spark application/driver
			 * adding into database
			 */
			StreamDriverManager samgr=new StreamDriverManager();
			samgr.addStreamDriver(driver,consumer , user);
			
			
			if(exitStatus != 0){
				System.err.println("Execute Script Failed with exit code "+exitStatus);
				LOG.error("Execute Script Failed with exit code "+exitStatus);
			}
			else{
				System.out.println("Execute Script Success exit code " +exitStatus);
				LOG.info("Execute Script Success exit code " +exitStatus);
			}
		}
		catch(Exception e)
		{
			System.err.println("Error: " + e);
			LOG.error("Error: " + e);
			return "Failed";
		}

		return "Success";
	}
	
	public void stopConsumer(String driverId,String user){
		final String stopScript=ConfigurationReader.getProperty("CONSUMER_STOP_SCRIPT");
		//final String stopScript="/home/zeasStream/Consumer-Stop.sh";
		try{
				Session session=createSSHSession();
				ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
				channelExec.setCommand("sh "+stopScript+" "+driverId);
				channelExec.connect();
				int exitStatus = channelExec.getExitStatus();
				System.out.println("sh "+stopScript+" "+driverId);
				BufferedReader br=new BufferedReader(new InputStreamReader(channelExec.getInputStream()));
				String text;
				
				while((text=br.readLine())!=null){
					System.out.println(text);
				}
				
							
				System.out.println("Consumer stopped succesfully.....................................");
				LOG.info("Consumer stopped succesfully.....................................");
		
				/*
				 * updating the spark application/driver status in database
				 */
				StreamDriverManager samgr=new StreamDriverManager();
				samgr.updateStreamDriver(driverId, "user");
				
				if(exitStatus != 0){
					System.err.println("Execute Script Failed with exit code "+exitStatus);
					LOG.error("Execute Script Failed with exit code "+exitStatus);
				}
				else{
					System.out.println("Execute Script Success exit code " +exitStatus);
					LOG.info("Execute Script Success exit code " +exitStatus);
				}
			}
			catch(Exception e)
			{
				System.err.println("Error: " + e);
				LOG.error("Error: " + e);
				
			}
		
	}

	private Session createSSHSession(){
		
		final String sparkHost= ConfigurationReader.getProperty("SPARK_HOSTNAME");
		final String sshPort=ConfigurationReader.getProperty("SSH_PORT");
		final String sparkUser=ConfigurationReader.getProperty("SPARK_USER");
		final String sparkPassword=ConfigurationReader.getProperty("SPARK_PASSWORD");
		
		/*final String sparkHost= "ec2-54-174-149-226.compute-1.amazonaws.com";
		final String sshPort="22";
		final String sparkUser="zeasStream";
		final String sparkPassword="zeasStream";*/
		
		Properties prop=new Properties();
		prop.setProperty("StrictHostKeyChecking", "no");
		JSch jsch = new JSch();
		Session session=null;

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
	
	
	
	/*public static void main(String args[]){
		StreamConsumerExecutor sce=  new StreamConsumerExecutor();
	//	String json="{\"Consumer_Name\":\"sri_consumer_2\",\"Hostname\":\"ec2-54-174-149-226.compute-1.amazonaws.com\",\"Port\":\"2181\",\"Group_ID\":\"grp\",\"Topic\":\"test\",\"Output_Location\":\"hdfs:/user/19491/iotdata/\",\"Batch_Duration\":\"60\"}";
	//	sce.startConsumer(new JSONObject(json));
		
		String driver="driver-20150603061109-0028";
		sce.stopConsumer(driver);
		
	}*/


	
}



