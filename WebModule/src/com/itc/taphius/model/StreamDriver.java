package com.itc.taphius.model;



public class StreamDriver extends Entity {

	public String driverId;
	public String consumerName;
	public String status;
	public String startAt;
	public String stopAt;
	public String startBy;
	public String stopBy;
	private int count;
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public String getDriverId() {
		return driverId;
	}
	public void setDriverId(String driverId) {
		this.driverId = driverId;
	}
	public String getConsumerName() {
		return consumerName;
	}
	public void setConsumerName(String consumerName) {
		this.consumerName = consumerName;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getStartAt() {
		return startAt;
	}
	public void setStartAt(String startAt) {
		this.startAt = startAt;
	}
	public String getStopAt() {
		return stopAt;
	}
	public void setStopAt(String stopAt) {
		this.stopAt = stopAt;
	}
	public String getStartBy() {
		return startBy;
	}
	public void setStartBy(String startBy) {
		this.startBy = startBy;
	}
	public String getStopBy() {
		return stopBy;
	}
	public void setStopBy(String stopBy) {
		this.stopBy = stopBy;
	}
	
}
