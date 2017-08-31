package com.itc.taphius.utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class which loads Configuration file from given location 
 * and provides accessor method for accessing values for given Key.
 * @author 16795
 *
 */
public class ConfigurationReader {
	public static Properties prop = new Properties();

	/**
	 * Init method loads the Property file.
	 */
	static {
		try{
			InputStream inputStream = ConfigurationReader.class.getClassLoader().getResourceAsStream("/config.properties");
			prop.load(inputStream);
		}catch(IOException ioE){
			ioE.printStackTrace();
		}
	}

	/**
	 * Accessor method for Property Value
	 * @param key {@link String} Name of the key for which value is requested.
	 * @return {@link String} property value.
	 */
	public static String getProperty(String key){
		return prop.getProperty(key);
	}

}
