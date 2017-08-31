package com.taphius.databridge.flumeExecutor;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

import com.itc.zeas.utility.utility.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taphius.databridge.ingestion.CustomSource;

/**
 * Class for starting the flume-ng agent for given agent.
 * For every ingestion definition this class takes care of
 * starting off new flume agent.
 * @author 16795
 *
 */
public class FlumeNGExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CustomSource.class); 
    
	private ProcessBuilder pb;
	private static final String flume_ng = "flume-ng";
	private static final String BASH = "/bin/bash";
	private static final String CONF_FILE = "-f ";


	/**
	 * Utility method takes agent name and conf file path and starts flume agent 
	 * process for this agent via flume-ng command
	 * @param agent {@link String} Name of the agent
	 * @param conf {@link String} flume.conf file path
	 */
	public void start(final String agent, final String conf){

		try {
			File errors = new File("/var/log/flume/error.txt");
			File output = new File("/var/log/flume/ProcessLog.txt");
			File flume_bin = new File(ConfigurationReader.getProperty("FLUME_BIN"));
	
			pb = new ProcessBuilder( BASH, flume_ng, "agent","-n "+agent, CONF_FILE+conf );
			pb.directory(flume_bin);
			// Redirect the errorstream
//			pb.redirectErrorStream(true);

			pb.redirectError(Redirect.INHERIT);
			pb.redirectOutput(Redirect.INHERIT);
			Process p = pb.start();
			logger.info("Started Java Processbuilder for flume agent with Pid - "+p.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
