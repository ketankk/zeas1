package com.itc.zeas.dashboard.daoimpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.itc.zeas.utility.connection.ConnectionUtility;

public class DashBordServiceDB {

	public ResponseEntity<Object> getDashboardDataFromDB(String searchType, String graphType,String userName,String frequency) {
		List<String> operationListForProfile = new ArrayList<>();
		List<String> operationListForRun = new ArrayList<>();
		operationListForProfile.add("CREATE");
		operationListForProfile.add("DELETE");
		operationListForProfile.add("UPDATE");
		operationListForRun.add("FAIL");
		operationListForRun.add("SUCCESS");
		operationListForRun.add("TERMINATE");
		ResponseEntity<Object> responseEntity = null;
		Map<String, Map<String, Long>> graphMap = new HashMap<String, Map<String, Long>>();
		Map<String, Map<String, Long>> finalGraphMap = new LinkedHashMap<String, Map<String, Long>>();
		String query = "select EXTRACT(YEAR FROM time_occured) as year,"
				+ "EXTRACT(MONTH FROM time_occured) as month,"
				+ "EXTRACT(DAY FROM time_occured) as date,"
				+ "(select component_value from component_key c where c.id=b.component_key_id) as comp,"
				+ "(select operation_value from operation_key a where a.id=b.operation_key_id) as opr,"
				+ "COUNT(*) as count_opr from activities b where action_user_id=(select id from user where name=?)"
				+ " group by year,month,date,comp,opr";
		Connection connection = ConnectionUtility.getConnection();
		PreparedStatement preparedStatement=null;
		ResultSet resultSet=null;
		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, userName);
		 resultSet = preparedStatement.executeQuery();
		 Map<String, Long> interMap=null;
			while (resultSet.next()) {
				int year = resultSet.getInt("year");
				int month = resultSet.getInt("month")-1;
				int date=resultSet.getInt("date");
				String component = resultSet.getString("comp");
				String operation = resultSet.getString("opr");
				Long count = resultSet.getLong("count_opr");
				if (searchType.equalsIgnoreCase(component)) {
					String key=year+"-"+month+"-"+date;
					if(graphMap.containsKey(key)){
						graphMap.get(key).put(operation, count);
					}else{
						interMap= new HashMap<>();
						interMap.put(operation, count);
					}
					if ("profileStatus".equalsIgnoreCase(graphType)) {
						if (!operationListForProfile.contains(operation)) {
							interMap.remove(operation);
						}else{
							graphMap.put(key, interMap);
						}
					} else if ("runStatus".equalsIgnoreCase(graphType)) {
						if (!operationListForRun.contains(operation)) {
							interMap.remove(operation);
						}else{
							graphMap.put(key, interMap);
						}
					}
				}
			}
			finalGraphMap=getDataByFrequency(graphMap,frequency);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			ConnectionUtility.releaseConnectionResources(resultSet,preparedStatement, connection);
		}
		responseEntity = new ResponseEntity<Object>(finalGraphMap, HttpStatus.OK);
		return responseEntity;
	}

	private Map<String, Map<String, Long>> getDataByFrequency(Map<String, Map<String, Long>> graphMap, String frequency) {
		Map<String, Map<String, Long>> finalGraphMap = new LinkedHashMap<String, Map<String, Long>>();
		switch (frequency.toLowerCase()) {
		case "weekly":
			Calendar presentDay=Calendar.getInstance();
			List<Date> dateList=new ArrayList<>();
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			presentDay.add(Calendar.DATE, -1);
			dateList.add(presentDay.getTime());
			for(int i=dateList.size()-1;i>=0;i--){
				Date dates=dateList.get(i);
				Calendar datesCal=Calendar.getInstance();
				datesCal.setTime(dates);
				String dateKey=datesCal.get(Calendar.YEAR)+"-"+datesCal.get(Calendar.MONTH)+"-"+datesCal.get(Calendar.DATE);
				System.out.println(dateKey+":"+graphMap.get(dateKey));
				finalGraphMap.put(extractDate(datesCal),graphMap.containsKey(dateKey)?graphMap.get(dateKey):new HashMap<String,Long>());
			}
			break;
		case "monthly":
			break;
		default:
			break;
		}
		return finalGraphMap;
	}

	private String extractDate(Calendar dates) {
		Calendar c=Calendar.getInstance();
		c.set(dates.get(Calendar.YEAR),dates.get(Calendar.MONTH),dates.get(Calendar.DATE));
		return c.getDisplayName(Calendar.MONTH,Calendar.SHORT, Locale.getDefault())+"-"+c.get(Calendar.DATE);
	}



}
 