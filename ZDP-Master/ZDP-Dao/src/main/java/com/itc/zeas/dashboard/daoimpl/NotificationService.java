package com.itc.zeas.dashboard.daoimpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.dashboard.model.NotificationInfo;

public class NotificationService {

	List<NotificationInfo> notificationUnreadObjectList =new ArrayList<>();
	List<NotificationInfo> notificationReadObjectList =new ArrayList<>();
	
	public List<NotificationInfo> getLatestNotification(String userName){

		Connection connection = null;
		String sqlQuery = " select id,component_key_id,operation_key_id,component_id,status_messege,time_occured from activities "
				+ "where (id) NOT IN (select activity_id from alert_view) and action_user_id=(select id from user where name='"+ userName + "')";
		String sqlQuery1="select id,component_key_id,operation_key_id,component_id,status_messege,time_occured from activities "
				+ "where (id) IN (select activity_id from alert_view) and action_user_id=(select id from user where name='"+ userName + "')";
		String updateQuery = "insert into alert_view(activity_id,user_id) select ?,id from user where name=?";
		PreparedStatement preparedStatement = null;
		PreparedStatement preparedStatement1 = null;
		PreparedStatement preparedStatement2 = null;
		ResultSet resultSet = null;
		ResultSet resultSet1 = null;
		
		try {
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sqlQuery);
			preparedStatement2=connection.prepareStatement(sqlQuery1);
			 resultSet = preparedStatement.executeQuery();
			 resultSet1=preparedStatement2.executeQuery();
			 getNotificationList(resultSet1,1);//values in alert view
			 getNotificationList(resultSet,0);//values not in alert view
			 preparedStatement1 = connection.prepareStatement(updateQuery);
			for(NotificationInfo notificationInfo:notificationUnreadObjectList){
				if(!notificationReadObjectList.contains(notificationInfo.getId())){
				preparedStatement1.setLong(1,notificationInfo.getId());
				preparedStatement1.setString(2, userName);
				preparedStatement1.addBatch();
				}
			}
			preparedStatement1.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
			ConnectionUtility.releaseConnectionResources(preparedStatement1, connection);
			ConnectionUtility.releaseConnectionResources(preparedStatement2, connection);
		}
		List<NotificationInfo> listOfTotalNotifications=new ArrayList<>();
		listOfTotalNotifications.addAll(notificationUnreadObjectList);
		listOfTotalNotifications.addAll(notificationReadObjectList);
		Collections.sort(listOfTotalNotifications);
		getNotificationObjects(listOfTotalNotifications);
		List<NotificationInfo> es=listOfTotalNotifications;
		if(listOfTotalNotifications.size()>50){
			es=listOfTotalNotifications.subList(0, 50);
		}
		return es;

	}

	private List<NotificationInfo> getNotificationList(ResultSet resultSet, int status) {
		List<NotificationInfo> notificationObjectList=new ArrayList<>();
		
		try{
		while (resultSet.next()) {
			NotificationInfo notificationEntity = new NotificationInfo();
			notificationEntity.setId(resultSet.getLong("id"));
			notificationEntity.setComponent_type(getComponent(resultSet.getInt("component_key_id")));
			notificationEntity.setOperation_type(getOperation(resultSet.getInt("operation_key_id")));
			notificationEntity.setComponent_id(resultSet.getLong("component_id"));
			notificationEntity.setStatus_message(resultSet
					.getString("status_messege"));
			notificationEntity.setTime_occured(Timestamp.valueOf(resultSet
					.getString("time_occured")));
			notificationObjectList.add(notificationEntity);
		}
		if(status==1){
			notificationReadObjectList=notificationObjectList;
		}else{
			notificationUnreadObjectList=notificationObjectList;
		}

		
		}catch(SQLException e){
			e.printStackTrace();
		}finally {
			if(resultSet!=null){
				try {
					resultSet.close();
				} catch (SQLException e) {
				}
			}
		}
		return notificationObjectList;
	}
	private List<NotificationInfo> getNotificationObjects(List<NotificationInfo> notificationObjectList){
		List<NotificationInfo> listOfNotifications = new ArrayList<>();
		Calendar presentTime = Calendar.getInstance();
		presentTime.setTime(new Date());
		for (NotificationInfo notification : notificationObjectList) {
			Calendar runDate = Calendar.getInstance();
			runDate.setTime(new Date(notification.getTime_occured()
					.getTime()));
			String am_pm = +runDate.get(Calendar.AM_PM) == 1 ? "pm" : "am";
			long e = Math.abs(presentTime.getTimeInMillis()
					- runDate.getTimeInMillis())
					/ (60 * 1000);
			String info = "";
			if (Math.abs(presentTime.get(Calendar.DATE)
					- runDate.get(Calendar.DATE)) < 2) {
				Double hour = (e / (double) 60);
				if (presentTime.get(Calendar.DATE)
						- runDate.get(Calendar.DATE) == 1) {
					info = "Yesterday at " + runDate.get(Calendar.HOUR)
							+ ":" + runDate.get(Calendar.MINUTE) + " "
							+ am_pm;
				} else if (hour < 24 && hour > 1) {
					info = hour.intValue() + " hour ago";
				} else if (hour < 1) {
					if (((hour / 60) * 1000) > 1) {
						info = new Double((hour / 60) * 1000).intValue()
								+ " minute ago";
					} else {
						info = "Just now";
					}
				}
			} else {
				info = runDate.get(Calendar.DATE)
						+ " "
						+ runDate.getDisplayName(Calendar.MONTH,
								Calendar.LONG, Locale.getDefault()) + " "
						+ runDate.get(Calendar.YEAR) + " at "
						+ runDate.get(Calendar.HOUR) + ":"
						+ runDate.get(Calendar.MINUTE) + "" + am_pm;
			}
			notification.setTimeInfo(info);
			listOfNotifications.add(notification);
		}
		return listOfNotifications;
	}

	public Integer getNoOfNotifications(String userName) {

		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		String sqlQuery = "select COUNT(*) from activities where (id) NOT IN (select activity_id from alert_view) and "
				+ "action_user_id=(select id from user where name='"+ userName + "')";
		int count = 0;
		try {
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sqlQuery);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				count = resultSet.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionUtility.releaseConnectionResources(resultSet, preparedStatement, connection);
		}
		return count;

	}
	
	private String getComponent(int comp){
		String component_key="";
		switch (comp) {
		case 1:
			component_key="INGESTION";
			break;
		case 2:
			component_key="PROJECT";
			break;

		default:
			component_key="INGESTION";
			break;
		}
		return component_key;
		
	}
	private String getOperation(int opr){
		String operation_key="";
		
		switch (opr) {
		case 1:
			operation_key="CREATE";
			break;
		case 2:
			operation_key="DELETE";
			break;
		case 3:
			operation_key="START";
			break;
		case 4:
			operation_key="FAIL";
			break;
		case 5:
			operation_key="SUCCESS";
			break;
		case 6:
			operation_key="TERMINATE";
			break;
		case 7:
			operation_key="UPDATE";
			break;
		default:
			operation_key="CREATE";
			break;
		}
		return operation_key;
		
	}
	
	public static void main(String[] args) throws SQLException {
		NotificationService notificationService=new NotificationService();
		//System.out.println(notificationService.getNoOfNotifications("user"));
		System.out.println(notificationService.getLatestNotification("user"));
		System.out.println(notificationService.getLatestNotification("user").size());
		
	}

}
