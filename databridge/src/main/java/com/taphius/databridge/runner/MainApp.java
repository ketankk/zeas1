package com.taphius.databridge.runner;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MainApp {

	final static Logger logger = Logger.getLogger(MainApp.class);
	public static String FLUME_CONF_DIR = "";

	public static void main(String[] args) {

		new ClassPathXmlApplicationContext("springapp-servlet.xml");
		logger.info("Application context loaded successfully.");
	}

}
