package com.itc.zeas.model;
/**
 * This model class holds information related to datasetpathdetials
 * like if encryption is enabled on cluster,
 * what is the encryptionZone path.
 * @author 16795
 *
 */
public class DatasetPathDetails {
	/**
	 * flag to denote if Transparent encryption 
	 * feature is available on this cluster.
	 */
	private boolean isEncryptionAvailable;
	/**
	 * HDFS target path to store datasets relative to this.
	 * Dataset path will be <code>datasetRootPath/{loggedinUser}/profileName</code>
	 */
	private String datasetRootPath;
	/**
	 * If user wants to encrypt the dataset, then data needs to be ingested into Encryption zone.
	 * Dataset path will be <code>encryptionZonePath/{loggedinUser}/profileName</code>
	 */
	private String encryptionZonePath;
	
	public boolean isEncryptionAvailable() {
		return isEncryptionAvailable;
	}
	public void setEncryptionAvailable(boolean isEncryptionAvailable) {
		this.isEncryptionAvailable = isEncryptionAvailable;
	}
	public String getDatasetRootPath() {
		return datasetRootPath;
	}
	public void setDatasetRootPath(String datasetRootPath) {
		this.datasetRootPath = datasetRootPath;
	}
	public String getEncryptionZonePath() {
		return encryptionZonePath;
	}
	public void setEncryptionZonePath(String encryptionZonePath) {
		this.encryptionZonePath = encryptionZonePath;
	}

}
