package com.itc.zeas.utility;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.IOException;

public class CommonResourceLoader {
	

/**
 * @author 16765
 * This method is to load DataPassFrequency.properties file 
 * Variables are added to ArrayList and returned as JSON object
 */
	
	public static List<String> frequencyLoader()
	{
		Properties prop = new Properties();
		InputStream input = null;
		List<String> frequecyList = new ArrayList<String>();
		try
		{
			//input = new FileInputStream("/datapassfrequency.properties");
			input = CommonResourceLoader.class.getClassLoader().getResourceAsStream("/datapassfrequency.properties");
			prop.load(input);
		
			frequecyList.add(prop.getProperty("daily"));
			frequecyList.add(prop.getProperty("hourly"));
			frequecyList.add(prop.getProperty("monthly"));
			frequecyList.add(prop.getProperty("yearly"));
			
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
		
		return frequecyList;
	}
	/**
	 * 
	 * @return list of schema types defined for the data
	 * list will get displayed in jsp page as drop down
	 */
	
	public static List<String> schemaTypeLoader()
	{
		Properties prop = new Properties();
		InputStream input = null;
		List<String> schemaList = new ArrayList<String>();
		try
		{
			input = CommonResourceLoader.class.getClassLoader().getResourceAsStream("/schematype.properties");
			prop.load(input);
		
			schemaList.add(prop.getProperty("file"));
			schemaList.add(prop.getProperty("image"));
			schemaList.add(prop.getProperty("xfile"));
			schemaList.add(prop.getProperty("ximage"));
			
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
		
		return schemaList;
	}

}
