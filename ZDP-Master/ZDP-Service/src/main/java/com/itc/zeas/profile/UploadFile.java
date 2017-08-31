package com.itc.zeas.profile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.itc.zeas.ingestion.automatic.TriggerScheduler;
import org.apache.poi.util.IOUtils;
import org.springframework.web.multipart.MultipartFile;

import com.itc.zeas.utility.utility.UserProfileStatusCache;


/**
 * This class UploadFile is used for creating multiple thread to copy uploaded file along with 
 * create sample file for preview. 
 * 
 * 
 * 
 * Extends Thread class.
 *
 *
 *
 @author Nihar
 *
 */
public class UploadFile extends Thread{
	private boolean isUpload;
	private InputStream fileInputStream;
	private FileOutputStream fileOutputStream;
	private String profileName; 
	private String userName;
	private String filPath;
	
	public UploadFile(MultipartFile multipartFile, boolean isUpload, String filePath, String profileName,String userName) {
		this.isUpload=isUpload;
		this.profileName=profileName;
		this.userName=userName;
		this.filPath=filePath;
		try {
			this.fileInputStream = new BufferedInputStream(multipartFile.getInputStream());
			fileOutputStream= new FileOutputStream(new File(filePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//ToDO instantiate using annotation
	TriggerScheduler triggerScheduler;
	
	@Override
	public void run() {
		try {
			// Copy the whole uploaded file after ingestion profile created 
			// along with that start the ingestion to hdfs and validation process.
		if(isUpload){
			IOUtils.copy(fileInputStream, fileOutputStream);
		}else{
			String key=userName+"-"+profileName;
			boolean isReady=false;
			long startTime = System.currentTimeMillis();
			while((System.currentTimeMillis()-startTime)<1200000){
				// Cheking corresponding profile is successfully created or not
				if(UserProfileStatusCache.getStatus(key)){
					isReady=true;
					break;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if(isReady){
				List<String> users=new ArrayList<>();
				users.add(userName);
				//TODO remove this
				try {
//triggerScheduler not initialised
					new TriggerScheduler().runSchedular(profileName,userName,users,true);
				} catch ( Exception e) {
					e.printStackTrace();
				}
			}else{
				// Removing temporary key after certain timeout.
				UserProfileStatusCache.removeKey(key);
				File file=new File(filPath);
				if(file.exists()){
					file.delete();
				}
			}
			

				}
		}catch (IOException e) {
					e.printStackTrace();
				}finally{
					try {
						if(fileInputStream!=null){
						fileInputStream.close();
						}
						if(fileOutputStream!=null){
						fileOutputStream.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
	}

}
