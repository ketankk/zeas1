package com.itc.zeas.dashboard.dashboard;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;


import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.glassfish.jersey.client.ClientResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.utility.utility.ConfigurationReader;

import sun.misc.BASE64Encoder;

public class DashboardService {

	public ClientResponse callGraylogHistogramApi(String searchType,
												  String logToBeSearched, String interval, String keyword,
												  HttpServletRequest httpServletRequest) {
		System.out.println("In callGraylogHistogramApi");
		CommonUtils commonUtils = new CommonUtils();

		String userName = commonUtils
				.extractUserNameFromRequest(httpServletRequest);
		// String userName = "jeff";

		String user = "\"User " + userName;
		String message = " " + logToBeSearched + "\"";
		String graylogUrl = null;
		String adminMsg = "\"" + logToBeSearched + "\"";

		String graylog_server = ConfigurationReader
				.getProperty("graylog_server");
		String name = ConfigurationReader.getProperty("graylog_username");
		String password = ConfigurationReader.getProperty("graylog_pwd");
		/*
		 * String graylog_server="54.174.149.226"; String name = "admin"; String
		 * password = "yourpassword";
		 */
		System.out.println("graylog_server:" + graylog_server);

		try {
			if (searchType.equals("admin")) {
				graylogUrl = "http://"
						+ graylog_server
						+ ":12900/search/universal/keyword/histogram?query=message:"
						+ URLEncoder.encode(adminMsg, "UTF-8") + "&interval="
						+ URLEncoder.encode(interval, "UTF-8") + "&keyword="
						+ URLEncoder.encode(keyword, "UTF-8");
			} else {
				// graylogUrl =
				// "http://10.6.116.179:12900/search/universal/keyword/histogram?query=message:"
				// graylogUrl =
				// "http://54.174.149.226:12900/search/universal/keyword/histogram?query=message:"
				graylogUrl = "http://"
						+ graylog_server
						+ ":12900/search/universal/keyword/histogram?query=message:"
						+ URLEncoder.encode(user, "UTF-8") + ":"
						+ URLEncoder.encode(message, "UTF-8") + "&interval="
						+ URLEncoder.encode(interval, "UTF-8") + "&keyword="
						+ URLEncoder.encode(keyword, "UTF-8");
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		System.out.println("graylogUrl:" + graylogUrl);
		String authString = name + ":" + password;
		String authStringEnc = new BASE64Encoder()
				.encode(authString.getBytes());
		System.out.println("Base64 encoded auth string: " + authStringEnc);
		Client restClient = ClientBuilder.newClient();
		ClientResponse resp = restClient.target(graylogUrl).request("application/json")
				.header("Authorization", "Basic " + authStringEnc)
				.get(ClientResponse.class);

		System.out.println("resp:" + resp);
		return resp;
	}

	public ClientResponse callGraylogMsgSearchApi(String logToBeSearched,
			HttpServletRequest httpServletRequest) {
		System.out.println("In callGraylogMsgSearchApi");
		CommonUtils commonUtils = new CommonUtils();

		String userName = commonUtils
				.extractUserNameFromRequest(httpServletRequest);
		// String userName = "jeff";

		String user = "\"User " + userName;
		String source = "\"Zeas ZDP-master\"";
		String message = " " + logToBeSearched + "\"";
		String keyword = "\"1 year ago\"";
		String graylogUrl = null;

		String graylog_server = ConfigurationReader
				.getProperty("graylog_server");
		String name = ConfigurationReader.getProperty("graylog_username");
		String password = ConfigurationReader.getProperty("graylog_pwd");

		try {
			// graylogUrl =
			// "http://10.6.116.179:12900/search/universal/keyword?query=source:"
			// graylogUrl =
			// "http://54.174.149.226:12900/search/universal/keyword?query=source:"
			graylogUrl = "http://" + graylog_server
					+ ":12900/search/universal/keyword?query=source:"
					+ URLEncoder.encode(source, "UTF-8") + "AND+message:"
					+ URLEncoder.encode(user, "UTF-8") + ":"
					+ URLEncoder.encode(message, "UTF-8") + "&keyword="
					+ URLEncoder.encode(keyword, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		String authString = name + ":" + password;
		String authStringEnc = new BASE64Encoder()
				.encode(authString.getBytes());
		System.out.println("Base64 encoded auth string: " + authStringEnc);
		Client restClient = ClientBuilder.newClient();
		ClientResponse resp = restClient.target(graylogUrl).request("application/json")
				.header("Authorization", "Basic " + authStringEnc)
				.get(ClientResponse.class);

		System.out.println("resp:" + resp);
		return resp;
	}

	public Map<String, Map<String, Integer>> parseGraylogHistogramResp(
			String type, String graylogResp,
			Map<String, Map<String, Integer>> graphMap) {
		System.out.println("In parseGraylogHistogramResp");
		Gson gSon = new GsonBuilder().create();
		Map<String, Map<String, Double>> respMap = gSon.fromJson(graylogResp,
				HashMap.class);
		System.out.println("respMap:" + respMap);
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		Map<String, Double> resultsMap = respMap.get("results");

		if (!resultsMap.isEmpty()) {
			for (Entry<String, Double> entry : resultsMap.entrySet()) {
				// Convert seconds to date
				System.out.println(entry.getKey() + "    "
						+ entry.getValue().intValue());
				String dateInSecs = entry.getKey();
				int count = entry.getValue().intValue();
				long dateInMillis = Long.parseLong(dateInSecs) * 1000;
				Date date = new Date(dateInMillis);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				int year = calendar.get(calendar.YEAR);
				int month = calendar.get(calendar.MONTH);
				month++;
				tempMap.put(year + "-" + month, count);
			}
			Map<String, Integer> tempMap2 = new HashMap<String, Integer>();
			String dateKey;
			Integer profileValue;
			for (Map.Entry<String, Integer> entry : tempMap.entrySet()) {
				tempMap2 = new HashMap<String, Integer>();
				dateKey = entry.getKey();
				profileValue = entry.getValue();
				tempMap2.put(type, profileValue);
				if (graphMap.containsKey(dateKey)) {
					Map<String, Integer> map = graphMap.get(dateKey);
					tempMap2.putAll(map);
					graphMap.put(dateKey, tempMap2);
				} else {
					graphMap.put(dateKey, tempMap2);
				}
			}
		}
		return graphMap;
	}

	public Map<String, Map<String, Integer>> parseGraylogMsgResp(String type,
			String graylogResp, Map<String, Map<String, Integer>> graphMap) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = null;
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		Map<String, Integer> tempMap2 = new HashMap<String, Integer>();

		try {
			jsonNode = mapper.readTree(graylogResp).get("messages");
		} catch (IOException e) {
			e.printStackTrace();
		}
		Iterator<JsonNode> array = jsonNode.iterator();
		while (array.hasNext()) {
			JsonNode jnode = array.next();
			String timestamp = jnode.get("message").get("timestamp")
					.getTextValue();
			String dateKey = timestamp.substring(0, 7);
			if (tempMap.containsKey(dateKey)) {
				tempMap.put(dateKey, tempMap.get(dateKey) + 1);
			} else {
				tempMap.put(dateKey, 1);
			}
		}
		String dateKey;
		Integer profileValue;
		for (Map.Entry<String, Integer> entry : tempMap.entrySet()) {
			tempMap2 = new HashMap<String, Integer>();
			dateKey = entry.getKey();
			profileValue = entry.getValue();
			tempMap2.put(type, profileValue);
			if (graphMap.containsKey(dateKey)) {
				Map<String, Integer> map = graphMap.get(dateKey);
				tempMap2.putAll(map);
				graphMap.put(dateKey, tempMap2);
			} else {
				graphMap.put(dateKey, tempMap2);
			}
		}
		return graphMap;
	}

	public ResponseEntity<Object> getDashboardDataFromGraylog(
			String searchType, HttpServletRequest httpServletRequest) {
		Map<String, Map<String, Integer>> graphMap = new HashMap<String, Map<String, Integer>>();
		ResponseEntity<Object> responseEntity = null;
		DashboardService dashService = new DashboardService();
		String newIngestionProfileOut = "";
		String ingestionCompletedOut = "";
		String ingestionFailedOut = "";

		String newProjectOut = "";
		String projectCompletedOut = "";
		String projectFailedOut = "";

		String logonSuccessOut = "";
		String logonFailedOut = "";
		String exceptionsOut = "";

		System.out.println("searchType:" + searchType);

		if (searchType.equals("ingestion")) {
			ClientResponse newIngestionProfileResp = dashService
					.callGraylogHistogramApi(searchType,
							"New ingestion profile", "month", "a year ago",
							httpServletRequest);
			newIngestionProfileOut = newIngestionProfileResp
					.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionProfileCreated", newIngestionProfileOut,
						graphMap);
			}

			ClientResponse ingestionCompletedResp = dashService
					.callGraylogHistogramApi(searchType, "Ingestion completed",
							"month", "a year ago", httpServletRequest);
			ingestionCompletedOut = ingestionCompletedResp
					.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionCompleted", ingestionCompletedOut, graphMap);
			}

			ClientResponse ingestionFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Ingestion failed",
							"month", "a year ago", httpServletRequest);
			ingestionFailedOut = ingestionFailedResp.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionFailed", ingestionFailedOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return responseEntity;
		}
		if (searchType.equals("project")) {
			ClientResponse newProjectResp = dashService
					.callGraylogHistogramApi(searchType, "New project",
							"month", "a year ago", httpServletRequest);
			newProjectOut = newProjectResp.readEntity(String.class);
			if (newProjectResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectCreated", newProjectOut, graphMap);
			}
			ClientResponse projCompletedResp = dashService
					.callGraylogHistogramApi(searchType, "Project completed",
							"month", "a year ago", httpServletRequest);
			projectCompletedOut = projCompletedResp.readEntity(String.class);
			if (projCompletedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectCompleted", projectCompletedOut, graphMap);
			}
			ClientResponse projFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Project failed",
							"month", "a year ago", httpServletRequest);
			projectFailedOut = projFailedResp.readEntity(String.class);
			if (projCompletedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectFailed", projectFailedOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return responseEntity;
		}
		if (searchType.equals("admin")) {
			ClientResponse logonSuccessResp = dashService
					.callGraylogHistogramApi(searchType, "Logon success",
							"month", "a year ago", httpServletRequest);
			logonSuccessOut = logonSuccessResp.readEntity(String.class);
			if (logonSuccessResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"logonSuccess", logonSuccessOut, graphMap);
			}
			ClientResponse logonFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Logon failed",
							"month", "a year ago", httpServletRequest);
			logonFailedOut = logonFailedResp.readEntity(String.class);
			if (logonFailedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp("logonFailed",
						logonFailedOut, graphMap);
			}
			ClientResponse exceptionsResp = dashService
					.callGraylogHistogramApi(searchType, "exception", "month",
							"a year ago", httpServletRequest);
			exceptionsOut = exceptionsResp.readEntity(String.class);
			if (exceptionsResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp("exceptions",
						exceptionsOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return responseEntity;
		}
		responseEntity = new ResponseEntity<Object>(graphMap,
				HttpStatus.NOT_FOUND);
		return responseEntity;
	}

	public static void main(String args[]) throws IOException {

		HttpServletRequest httpServletRequest = null;
		String searchType = "admin";
		Map<String, Map<String, Integer>> graphMap = new HashMap<String, Map<String, Integer>>();
		ResponseEntity<Object> responseEntity;
		DashboardService dashService = new DashboardService();
		String newIngestionProfileOut = "";
		String ingestionCompletedOut = "";
		String ingestionFailedOut = "";
		String newProjectOut = "";
		String projectCompletedOut = "";
		String projectFailedOut = "";
		String logonSuccessOut = "";
		String logonFailedOut = "";
		String exceptionsOut = "";

		if (searchType.equals("ingestion")) {
			ClientResponse newIngestionProfileResp = dashService
					.callGraylogHistogramApi(searchType,
							"New ingestion profile", "month", "a year ago",
							httpServletRequest);
			newIngestionProfileOut = newIngestionProfileResp
					.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionProfileCreated", newIngestionProfileOut,
						graphMap);
			}

			ClientResponse ingestionCompletedResp = dashService
					.callGraylogHistogramApi(searchType, "Ingestion completed",
							"month", "a year ago", httpServletRequest);
			ingestionCompletedOut = ingestionCompletedResp
					.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionCompleted", ingestionCompletedOut, graphMap);
			}

			ClientResponse ingestionFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Ingestion failed",
							"month", "a year ago", httpServletRequest);
			ingestionFailedOut = ingestionFailedResp.readEntity(String.class);
			if (newIngestionProfileResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"ingestionFailed", ingestionFailedOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return;
		}
		if (searchType.equals("project")) {
			ClientResponse newProjectResp = dashService
					.callGraylogHistogramApi(searchType, "New project",
							"month", "a year ago", httpServletRequest);
			newProjectOut = newProjectResp.readEntity(String.class);
			if (newProjectResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectCreated", newProjectOut, graphMap);
			}
			ClientResponse projCompletedResp = dashService
					.callGraylogHistogramApi(searchType, "Project completed",
							"month", "a year ago", httpServletRequest);
			projectCompletedOut = projCompletedResp.readEntity(String.class);
			if (projCompletedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectCompleted", projectCompletedOut, graphMap);
			}
			ClientResponse projFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Project failed",
							"month", "a year ago", httpServletRequest);
			projectFailedOut = projFailedResp.readEntity(String.class);
			if (projCompletedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"projectFailed", projectFailedOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return;
		}
		if (searchType.equals("admin")) {
			ClientResponse logonSuccessResp = dashService
					.callGraylogHistogramApi(searchType, "Logon success",
							"month", "a year ago", httpServletRequest);
			logonSuccessOut = logonSuccessResp.readEntity(String.class);
			if (logonSuccessResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp(
						"logonSuccess", logonSuccessOut, graphMap);
			}
			ClientResponse logonFailedResp = dashService
					.callGraylogHistogramApi(searchType, "Logon failed",
							"month", "a year ago", httpServletRequest);
			logonFailedOut = logonFailedResp.readEntity(String.class);
			if (logonFailedResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp("logonFailed",
						logonFailedOut, graphMap);
			}
			ClientResponse exceptionsResp = dashService
					.callGraylogHistogramApi(searchType, "exception", "month",
							"a year ago", httpServletRequest);
			exceptionsOut = exceptionsResp.readEntity(String.class);
			if (exceptionsResp.getStatus() == 200) {
				graphMap = dashService.parseGraylogHistogramResp("exceptions",
						exceptionsOut, graphMap);
			}
			responseEntity = new ResponseEntity<Object>(graphMap, HttpStatus.OK);
			return;
		}
	}
}
