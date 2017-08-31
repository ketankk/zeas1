package com.taphius.databridge.deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DataSourcerConfigDetails<T> {
	
	/**
	 * Google gSon instance
	 */
	Gson gSon;
	
	private Class<T> type;
	/**
	 * Instance holds all DataSourcer Configuration details 
	 * read from DB.
	 * This details will be used for configuring Flume job for Data ingestion.
	 */
	T configDetails;
	
	public DataSourcerConfigDetails(Class<T> cls){
		this.gSon = new GsonBuilder().create();
		this.type = cls;
	}
	
	Class<T> getType() {
		return type;
	}
	
	/**
	 * Returns {@link DataSourcerConfigDetails} instance populated with details
	 * @return instance {@link DataSourcerConfigDetails}
	 */
	public T getDSConfigDetails(String string){
		parserJSONBlob(string);
		return configDetails;
	}
	
	private void parserJSONBlob(String string){		
	//	blob = "{\"dataSourcerId\":\"dsOne\",\"location\":\"\\var\\log\\dumpData\",\"frequency\":\"1\",\"schema\":\"xml\"}";

		configDetails = gSon.fromJson(string, this.getType());
	}	
	


}
