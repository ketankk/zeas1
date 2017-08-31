package com.itc.zeas.model;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.itc.zeas.ingestion.model.OozieJobStatus;
import com.itc.zeas.utility.connection.ConnectionUtility;

public class OozieIngestionStatus {

	/*public static void main(String[] args) throws IOException {
		OozieStatus oozieStatus = new OozieStatus();

		String d=oozieStatus.getStatusJson("dataset_scheduler_project_dataset");
		System.out.println(d);
	}*/

	private String getDatasetName(int Id){
		String name="";
		
		Connection dbConn = ConnectionUtility.getConnection();
		
		String dataSetNameQuery="SELECT dataset FROM scheduler WHERE project_id=?";
		PreparedStatement statement=null;
		try {
			statement=dbConn.prepareStatement(dataSetNameQuery);
			statement.setInt(1, Id);
			ResultSet res = statement.executeQuery();
			while(res.next()){
				name=res.getString("dataset");
			}
			
		} catch (SQLException e) {
			
		}

		
		return name;
	}
	
	public String getStatusJson(String projName,int projId)   {
		//use config file for this url
		String data = null;
		String datasetName=getDatasetName(projId);
		
		String workflowName=projName+"_"+datasetName;
		System.out.println(workflowName+"...workflow");
		try{

		String oozieUrl;

		Properties prop = new Properties();
		FileInputStream inputStream = new FileInputStream(System.getProperty("user.home")
				+ "/zeas/Config/config.properties");
		prop.load(inputStream);
		String param="OOZIE_JOBS_API_URL";
		oozieUrl = prop.getProperty(param);
		
//System.out.println(oozieUrl+" oozieUrl");
		
		data= get(oozieUrl);
		} catch (IOException e) {
			e.printStackTrace();
		}
		JsonParser parser = new JsonParser();

		JsonArray jsonArray = parser.parse(data).getAsJsonObject().getAsJsonArray("workflows");

		Gson gson = new Gson();
		Type type = new TypeToken<List<OozieJobStatus>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		}.getType();

		String dataa = jsonArray.toString();
		List<OozieJobStatus> jobStatus = gson.fromJson(dataa, type);

		//System.out.println(jobStatus.toString());

		List<OozieJobStatus> reqJobList=new ArrayList<>();
		for (OozieJobStatus status : jobStatus) {
			if(status.getAppName().equals(workflowName))
				reqJobList.add(status);
			
			

		}
String jsonData=gson.toJson(reqJobList,type);
		return jsonData;
	}

	private String get(String oozieurl) throws IOException {
		URL url = new URL(oozieurl);

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		String output;
		String daa = "";
		while ((output = br.readLine()) != null) {
			daa = daa + output;
		}

		conn.disconnect();
		return daa;

	}

}
