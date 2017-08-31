package com.zdp.kafka.prducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;



public class ShellScriptExecutor {
	public static Logger LOG = Logger.getLogger(ShellScriptExecutor.class);
	public static final String BASH = "/bin/bash";

	public int runScript(String...args){
		LOG.info("Going to execute shell script - "+args[1]);   


		ProcessBuilder  pb = new ProcessBuilder(args);
		// Redirect the errorstream
		pb.redirectErrorStream(true);
		pb.redirectErrorStream(true);
		Process p;
		try {
			p = pb.start();

			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			System.out.println(p.waitFor());
			System.out.println("br output =="+br.toString() + "=="+br.readLine());
			while (br.ready()) {
				String str=br.readLine().trim();
				System.out.println("str=="+str);
			}   
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
			ioe.printStackTrace();
		} 
		catch (InterruptedException ie) {
			// TODO Auto-generated catch block
			ie.printStackTrace();
		} 
		return 0;
	}

}
