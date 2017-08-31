package com.itc.zeas.utility.testrun;

/**
 * POJO class representing parameter required for Map Reduce test run.
 * 
 * @author 19217
 * 
 */
public class MapRedParam {
    private String stageName;
	private String mapperCName;
	private String reducerCName;
	private String mapRedJarPath;
	private String ipDataSetName;

	public String getMapperCName() {
		return mapperCName;
	}

	public void setMapperCName(String mapperCName) {
		this.mapperCName = mapperCName;
	}

	public String getReducerCName() {
		return reducerCName;
	}

	public void setReducerCName(String reducerCName) {
		this.reducerCName = reducerCName;
	}

	public String getMapRedJarPath() {
		return mapRedJarPath;
	}

	public void setMapRedJarPath(String mapRedJarPath) {
		this.mapRedJarPath = mapRedJarPath;
	}

	public String getIpDataSetName() {
		return ipDataSetName;
	}

	public void setIpDataSetName(String ipDataSetName) {
		this.ipDataSetName = ipDataSetName;
	}   

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }
}
