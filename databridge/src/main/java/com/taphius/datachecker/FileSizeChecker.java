package com.taphius.datachecker;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.zdp.dao.ZDPDataAccessObjectImpl;

public class FileSizeChecker
{
  String mintype;
  String maxtype;
  String location;
  int minSize;
  int maxSize;
  boolean notifyAlert;
  boolean notifyEmail;
  boolean ingestionStatus;
  String minFile;
  String maxFile;
  public HashMap<String, String> map = new HashMap<String, String>();
  public static Logger LOG = Logger.getLogger(FileSizeChecker.class);
  
  public HashMap<String, String> JSONData(JSONObject jsonObj)
  {
    try
    {
      JSONObject fileData = jsonObj.getJSONObject("fileData");
   
      if (fileData.getBoolean("notificationSet"))
      {
        String mintype = fileData.getString("mintype");
        this.mintype = mintype;
        
        String maxtype = fileData.getString("maxtype");
        this.maxtype = maxtype;
        
        int minSize = Integer.parseInt(fileData.getString("minsize"));
        this.minSize = minSize;
        
        int maxSize = Integer.parseInt(fileData.getString("maxsize"));
        this.maxSize = maxSize;
        
        boolean notifyAlert = fileData.getBoolean("notifyAlert");
        this.notifyAlert = notifyAlert;
        this.map.put("notifyAlert", Boolean.toString(notifyAlert));
        
        boolean notifyEmail = fileData.getBoolean("notifyEmail");
        this.notifyEmail = notifyEmail;
        this.map.put("notifyEmail", Boolean.toString(notifyEmail));
               
        this.minFile = (minSize + " " + mintype);
        this.map.put("minFile", this.minFile);
        this.maxFile = (maxSize + " " + maxtype);
        this.map.put("maxFile", this.maxFile);
       // return this.map;
      }
      boolean ingestionStatus = fileData.getBoolean("contIngestion");
      this.ingestionStatus = ingestionStatus;
      this.map.put("ingestionStatus", Boolean.toString(ingestionStatus));
      System.out.println("ingestionStatus inside jsonData =====" + ingestionStatus);
      return this.map;
    }
    catch (JSONException ex)
    {
      System.out.println("entering catch block");
    }
    return new HashMap();
  }
  
  public static long getBytesFromFormattedSize(String size)
    throws NumberFormatException
  {
    String[] arr = size.toUpperCase().split(" ");
    if (arr.length != 2)
    {
      if ((arr.length == 1) && (Character.isDigit(size.charAt(0)))) {
        return getBytesFromSize(size);
      }
      throw new IllegalArgumentException("Expected '<size> <unit>', got '" + size + "'");
    }
    char unit = arr[1].charAt(0);
    if (unit == 'B') {
      return Integer.parseInt(arr[0]);
    }
    int bytes = (arr[1].length() == 3) && (arr[1].charAt(1) == 'I') ? 1000 : 1024;
    char[] units = { 'K', 'M', 'G', 'T', 'P', 'E' };
    int exp = 1;
    for (char ch : units)
    {
      if (unit == ch) {
        break;
      }
      exp++;
    }
    return (long) (Double.parseDouble(arr[0]) * Math.pow(bytes, exp));
  }
  
  private static long getBytesFromSize(String size)
  {
    String fixedSize = "";
    for (char c : size.toCharArray())
    {
      if ((Character.isLetter(c)) && (!fixedSize.contains(" "))) {
        fixedSize = fixedSize + ' ';
      }
      fixedSize = fixedSize + c;
    }
    return getBytesFromFormattedSize(fixedSize);
  }
  
  public boolean getFileSizeCount(File file, String uploadType, String userName)
  {
    String entityName = uploadType;
    File[] files = file.listFiles();
    for (File f1 : files) {
      if ((!f1.isDirectory()) && (!f1.getName().equalsIgnoreCase("_DONE")))
      {
        ZDPDataAccessObjectImpl accessObjectActivity = new ZDPDataAccessObjectImpl();
        List<String> users = new ArrayList();
        Long schedularId = Long.valueOf(0L);
        try
        {
          users = accessObjectActivity.getUserListForGivenId(schedularId.toString(), "ingestion");
        }
        catch (Exception e1)
        {
          e1.printStackTrace();
        }
        Byte val = Byte.valueOf((byte)(int)f1.length());
        if (userName != null)
        {
          users.add(userName);
          String jsonStr = accessObjectActivity.getJSONFromEntity(entityName + "_Source");
          
          System.out.println("jsonStr =====" + jsonStr);
          if (jsonStr != null)
          {
            JSONObject jsonObj = new JSONObject(jsonStr);
            HashMap<String, String> map = JSONData(jsonObj);
            if ((map != null) && (!map.isEmpty()))
            {
            	if(!(map.size()==1)){
              Long minFileSize = Long.valueOf(getBytesFromFormattedSize((String)map.get("minFile")));
              Long maxFileSize = Long.valueOf(getBytesFromFormattedSize((String)map.get("maxFile")));
              
              Boolean notifyAlert = Boolean.valueOf(Boolean.parseBoolean((String)map.get("notifyAlert")));
              Boolean notifyEmail = Boolean.valueOf(Boolean.parseBoolean((String)map.get("notifyEmail")));
              
              String statusInfo = "FileSize of  " + entityName + "is :" + val + "in Bytes. It should be between " + (String)map.get("minFile") + "to" + (String)map.get("maxFile");
              System.out.println("statusInfo inside FileSizeChecker" + statusInfo);
              if ((val.byteValue() < minFileSize.longValue()) || (val.byteValue() > maxFileSize.longValue()))
              {
                if (notifyAlert.booleanValue()) {
                  try
                  {
                    accessObjectActivity.addActivitiesBatchForNewAPI(entityName.toString(), statusInfo, "ingestion", "SUCCESS", users, userName);
                  }
                  catch (SQLException e)
                  {
                    e.printStackTrace();
                  }
                }
                if (notifyEmail.booleanValue()) {
                  System.out.println("Notify Email need to take care in next sprint");
                }
              }
            }
            }
            
          }
        }
        if(Boolean.parseBoolean(map.get("ingestionStatus"))== false){
        	String statusInfo="Ingestion is not allowed for , "+ entityName.toString() +" since,its is not in the specified range";
        	try {
				accessObjectActivity.addActivitiesBatchForNewAPI(entityName.toString(), statusInfo, "ingestion", "FAIL", users, userName);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
      }
    }
    return Boolean.parseBoolean(map.get("ingestionStatus"));
  }
  
}
