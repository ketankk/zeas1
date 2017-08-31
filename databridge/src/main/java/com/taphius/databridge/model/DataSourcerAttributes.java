package com.taphius.databridge.model;

public class DataSourcerAttributes {
	
	private String dataSource;
	private String name;
	private String frequency;
	private String destinationDataset;
	private String id;
	/**
	 * This is reference to TargetDataSet {@link DataSet} POJO
	 */
	private DataSet dataSet;
	/**
	 * This is reference to DataSource {@link DataSource} POJO
	 */
	private DataSource dataSrc;
	
	public String getDestinationDataset() {
		return destinationDataset;
	}
	public void setDestinationDataset(String destinationDataset) {
		this.destinationDataset = destinationDataset;
	}
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSourcerId) {
		this.dataSource = dataSourcerId;
	}
	public String getLocation() {
		return dataSrc.getLocation();
	}	
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public String getSchema() {
		return this.dataSrc.getSchema();
	}	
	
	public String getFormat() {
		return this.dataSrc.getType();
	}	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public DataSet getDataSet() {
		return dataSet;
	}
	public void setDataSet(DataSet dataSet) {
		this.dataSet = dataSet;
	}
	public DataSource getDataSrc() {
		return dataSrc;
	}
	public void setDataSrc(DataSource dataSrc) {
		this.dataSrc = dataSrc;
	}
	
	@Override
	public String toString() {
		return this.getDataSource()+"=="+this.getLocation();
	}
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
   
}
