package com.itc.zeas.utility.utility;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Creates a map for keeping the info about ingestion profile creation status.
 * it is used in case of local file upload to check whether profile created or not.
 * @author Nihar
 *
 */
public class UserProfileStatusCache {
	
	private static Map<String, Boolean>  profileStatusCacheMap=new HashMap<>();
	
	
	/**
	 * Add the ingestion profile key for the first time 
	 * in case of local file upload
	 * @param key is a composite key made by username-profilename
	 */
	
	public static void addKeyToMap(String key){
		profileStatusCacheMap.put(key, false);
	}
	
	/**
	 * Update the status of ingestion profile creation.
	 * it will check the key present or not and update it's status
	 * @param key is a composite key made by username-profilename
	 * @param status is true if profile created or else false
	 */
	public static void updateMap(String key,Boolean status){
		if(profileStatusCacheMap.containsKey(key))
		profileStatusCacheMap.put(key, status);
	}
	
	
	/**
	 * returns true if ingestion profile is created in by user. else false.
	 * @param key is a composite key made by username-profilename
	 * @return ingestion profile creation status.
	 */
	public static boolean getStatus(String key){
		return profileStatusCacheMap.get(key);
	}
	
	/**
	 * Remove the key after completion of ingestion or after timeout
	 * @param key is a composite key made by username-profilename
	 * @return true if key removed.
	 */
	public static boolean removeKey(String key){
		return profileStatusCacheMap.remove(key);
	}
}
