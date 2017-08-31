package com.taphius.databridge.model;

import java.sql.Date;

public class DataIngestionLog {
    

    private int id;
    private int data_ingestion_id;    
    private String listoffiles;
    private String job_status;
    private String batch;
    private Date job_start_time;
    private Date job_end_time;
    private String job_msg;
    private String created_by;
    private Date createdDate;
    private String updated_by;
    private Date lastmodified;
//  private DataIngestion dataIngestion;
    
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }
    /**
     * @return the data_ingestion_id
     */
    public int getData_ingestion_id() {
        return data_ingestion_id;
    }
    /**
     * @param data_ingestion_id the data_ingestion_id to set
     */
    public void setData_ingestion_id(int data_ingestion_id) {
        this.data_ingestion_id = data_ingestion_id;
    }
    /**
     * @return the listoffiles
     */
    public String getListoffiles() {
        return listoffiles;
    }
    /**
     * @param listoffiles the listoffiles to set
     */
    public void setListoffiles(String listoffiles) {
        this.listoffiles = listoffiles;
    }
    /**
     * @return the job_status
     */
    public String getJob_status() {
        return job_status;
    }
    /**
     * @param job_status the job_status to set
     */
    public void setJob_status(String job_status) {
        this.job_status = job_status;
    }
    /**
     * @return the batch
     */
    public String getBatch() {
        return batch;
    }
    /**
     * @param batch the batch to set
     */
    public void setBatch(String batch) {
        this.batch = batch;
    }
    /**
     * @return the job_start_time
     */
    public Date getJob_start_time() {
        return job_start_time;
    }
    /**
     * @param job_start_time the job_start_time to set
     */
    public void setJob_start_time(Date job_start_time) {
        this.job_start_time = job_start_time;
    }
    /**
     * @return the job_end_time
     */
    public Date getJob_end_time() {
        return job_end_time;
    }
    /**
     * @param job_end_time the job_end_time to set
     */
    public void setJob_end_time(Date job_end_time) {
        this.job_end_time = job_end_time;
    }
    /**
     * @return the job_msg
     */
    public String getJob_msg() {
        return job_msg;
    }
    /**
     * @param job_msg the job_msg to set
     */
    public void setJob_msg(String job_msg) {
        this.job_msg = job_msg;
    }
    /**
     * @return the createdBy
     */
    public String getCreated_by() {
        return created_by;
    }
    /**
     * @param created_by the createdBy to set
     */
    public void setCreated_by(String created_by) {
        this.created_by = created_by;
    }
    /**
     * @return the createdDate
     */
    public Date getCreatedDate() {
        return createdDate;
    }
    /**
     * @param createdDate the createdDate to set
     */
    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }
    /**
     * @return the updatedBy
     */
    public String getUpdated_by() {
        return updated_by;
    }
    /**
     * @param updated_by the updatedBy to set
     */
    public void setUpdated_by(String updated_by) {
        this.updated_by = updated_by;
    }
    /**
     * @return the lastmodified
     */
    public Date getLastmodified() {
        return lastmodified;
    }
    /**
     * @param lastmodified the lastmodified to set
     */
    public void setLastmodified(Date lastmodified) {
        this.lastmodified = lastmodified;
    }

   
    

}
