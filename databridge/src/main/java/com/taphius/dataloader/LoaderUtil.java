package com.taphius.dataloader;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;








public class LoaderUtil {
    
    private static Logger logger = Logger.getLogger(LoaderUtil.class.getName());
    
    public static boolean verifyFileTransferStatus(Set<Future<Integer>> futureSet){
        boolean isSuccess = false;
        for (Future<Integer> future : futureSet) {
            while(!future.isDone()){
            }
            //Check if task is executed successfully
            try {
                if(future.get()==0){
                	//remove the break point
                   isSuccess=true;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.toString());
            }
        }        
        return isSuccess;

    }
    
    public static String getBatchID(String freq) {
        String pattern = "yyyyMMddHHmm";
        if(freq.equalsIgnoreCase("hourly")){
          pattern = pattern+"HH";        
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String batchId = sdf.format(new Date());
        
       /* if((freq.equalsIgnoreCase("daily")) || (freq.equalsIgnoreCase("weekly")) || freq.equalsIgnoreCase("onetime"))
            batchId+= "00";*/
        return batchId;
      }
    
    public static Timestamp getIngestionTime(){
    	
		return new Timestamp(System.currentTimeMillis());
    }
    
    /**
	 * method to get full details of ingestion.
	 * @param listOfFiles
	 * @param existFiles
	 * @param failedFiles
	 * @return message
	 */
	
	public static String getDetailedMessege(File[] listOfFiles, String existFiles, String failedFiles,List<String> duplicateFiles) {
		StringBuilder message = new StringBuilder();
		List<String> existFile = new ArrayList<>(Arrays.asList(existFiles.split(",")));
		List<String> failedFile = new ArrayList<>(Arrays.asList(failedFiles.split(",")));
		for (File f : listOfFiles) {
			if (existFile.contains(f.getName())) {
				message.append(f.getName() + "@"
						+ "Fail@ (File exists/hadoop issue)" + "#");
			} else if (duplicateFiles.contains(f.getName())) {
				message.append(f.getName()+"@Fail@Duplicate File#");
			} else {
				message.append(f.getName() + "@" + "Success @ " + "#");
			}
		}
		if(!failedFiles.isEmpty()){
			for(String fileName:failedFile){
			message.append(fileName + "@" + "Fail@(File type mismatch)" + "#");
			}
		}
		if (message.length() > 0) {
			message.delete((message.length() - 1), message.length());
		}
		//adding duplicat file info
	/*	System.out.println("duplicate files list:"+duplicateFiles);
		for(String file:duplicateFiles){
			message.append("#"+file+"@fail@duplicate file, already ingested");
		}*/
		
		System.out.println("Meesege *********************  "+message);
		return message.toString();
	}
	
	/*
	 * it returns md5 values for file. which helps to determine difference in files
	 */
	
	public static String getMD5(File fileName) {

		String hashCodeStr = "";
		try {
			HashCode hs = Files.hash(fileName, Hashing.md5());
			hashCodeStr = hs.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashCodeStr;
	}
  }
