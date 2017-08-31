package com.taphius.databridge.dao;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.itc.zeas.utility.DBUtility;
import com.taphius.validation.mr.IngestionLogDetails;

public class IngestionLogDAO {

	// private Connection connection;

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws IOException
	 * @throws SQLException
	 */
	public void addIngestionLogEntry(int ingestionId, String batchId,
			String listOFFiles, String stage, String status)
			throws IOException, SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_LISTOFFILES");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionId);
			preparedStatement.setString(2, batchId);
			preparedStatement.setString(3, listOFFiles);
			preparedStatement.setString(4, stage);
			preparedStatement.setString(5, status);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws IOException
	 * @throws SQLException
	 */
	public void addIngestionLogEntry(int ingestionId, String batchId,
			String listOFFiles, String stage, String status, String message)
			throws IOException, SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_MESSAGE");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionId);
			preparedStatement.setString(2, batchId);
			preparedStatement.setString(3, listOFFiles);
			preparedStatement.setString(4, stage);
			preparedStatement.setString(5, status);
			preparedStatement.setString(6, message);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	public void updateIngestionLogEntry(int ingestionId, String batchId,
			Timestamp start, Timestamp end, String status) throws IOException,
			SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("UPDATE_INGESTION_LOG");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, status);
			preparedStatement.setTimestamp(2, start);
			preparedStatement.setTimestamp(3, end);
			preparedStatement.setInt(4, ingestionId);
			preparedStatement.setString(5, batchId);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	public void cleanUpLog(int ingestionId) throws IOException, SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("DELETE_INGESTION_LOG");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionId);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}

	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws IOException
	 * @throws SQLException
	 */
	public void addIngestionLogDetails(int ingestionId, String batchId,
			String listOFFiles, String stage, String status, String message,
			Timestamp updateTime) throws IOException, SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility
					.getSQlProperty("INSERT_LOGMESSAGETIMESTAMP");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionId);
			preparedStatement.setString(2, batchId);
			preparedStatement.setString(3, listOFFiles);
			preparedStatement.setString(4, stage);
			preparedStatement.setString(5, status);
			preparedStatement.setString(6, message);
			preparedStatement.setTimestamp(7, updateTime);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	/**
	 * this method is to add entity to database
	 * 
	 * @param entity
	 * @throws IOException
	 * @throws SQLException
	 */
	public void addIngestionLogDetails(int ingestionId, String batchId,
			String listOFFiles, String stage, String status,
			Timestamp updateTime) throws IOException, SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_LOGTIMESTAMP");
			connection = DBUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, ingestionId);
			preparedStatement.setString(2, batchId);
			preparedStatement.setString(3, listOFFiles);
			preparedStatement.setString(4, stage);
			preparedStatement.setString(5, status);
			preparedStatement.setTimestamp(6, updateTime);

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(preparedStatement, connection);
		}
	}

	public void addLogObject(int ingestionId, String batchId, String stage,
			String status, IngestionLogDetails logDetails) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			String sql = "insert into data_ingestion_log(DATA_INGESTION_ID,BATCH,listOfFiles,JOB_STATUS,JOB_STAGE,log_object) values(?,?,?,?,?,?)";
			// update data_ingestion_log set
			// job_status=?,job_stage=?,log_object=?
			connection = DBUtility.getConnection();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeObject(logDetails);
			os.flush();
			byte[] byteData = bos.toByteArray();
			os.close();
			bos.close();
			ps = connection.prepareStatement(sql);
			ps.setInt(1, ingestionId);
			ps.setString(2, batchId);
			ps.setString(3, "");// list of files
			ps.setString(4, status);
			ps.setString(5, stage);
			ps.setObject(6, byteData);
			ps.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(ps, connection);
		}
	}

	public void updateLogObject(int ingestionId, String batchId, String stage,
			String status, IngestionLogDetails logDetails) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			String sql = "update data_ingestion_log elog1,(select max(DATA_INGESTION_LOG_ID) as mid from data_ingestion_log  where "
					+ " DATA_INGESTION_ID="
					+ ingestionId
					+ " group by DATA_INGESTION_ID) elog2 set JOB_STAGE=?, JOB_STATUS=?, log_object=?  "
					+ "where elog1.DATA_INGESTION_LOG_ID=elog2.mid";
			connection = DBUtility.getConnection();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);

			os.writeObject(logDetails);
			os.flush();
			os.close();
			bos.close();
			byte[] data = bos.toByteArray();
			ps = connection.prepareStatement(sql);
			ps.setString(1, stage);
			ps.setString(2, status);
			ps.setObject(3, data);
			ps.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(ps, connection);
		}
	}

	public IngestionLogDetails getLogObject(int ingestionId) {

		IngestionLogDetails logs = new IngestionLogDetails();
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select * from data_ingestion_log where DATA_INGESTION_LOG_ID = (select max(DATA_INGESTION_LOG_ID) from "
				+ "data_ingestion_log where DATA_INGESTION_ID= ?) ";
		try {
			connection = DBUtility.getConnection();
			ps = connection.prepareStatement(sql);
			ps.setInt(1, ingestionId);
			rs = ps.executeQuery();
			if (rs.next()) {
				ByteArrayInputStream bais;
				ObjectInputStream ins;
				byte[] byteArray = rs.getBytes("log_object");
				if (byteArray != null) {
					bais = new ByteArrayInputStream(byteArray);
					ins = new ObjectInputStream(bais);
					logs = (IngestionLogDetails) ins.readObject();
					ins.close();
				}
				// System.out.println("Object in value ::"+mc.getSName());

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(ps, connection);
		}

		return logs;
	}

	public static void main(String[] args) {

		IngestionLogDetails logs = new IngestionLogDetails();
		// logs.setIngestionStart("ingestion started..22222...");
		// logs.setIngestionComplete("ingestion complted");
		logs.setValidationStart("validation startttt  222222222");
		IngestionLogDAO dao = new IngestionLogDAO();
		// Connection connection=dao.getConnection();
		// dao.addLogObject(9999, "999A", "ingestion", "inprogress", logs);
		// dao.updateLogObject(9999, "999A", "ingestion", "inprogress", logs);
		IngestionLogDetails temp = dao.getLogObject(10836);
		System.out.println(temp.toString());
		// System.out.println("end");

	}

	public void addGraylogInfo(Timestamp timestamp, String user, String type,
			String entityname, String operation) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			String sql = "insert into run_info(TimeStamp,user,type,entityname,operation) values(?,?,?,?,?)"; // insert_graylog_info
			connection = DBUtility.getConnection();
			ps = connection.prepareStatement(sql);
			ps.setTimestamp(1, timestamp);
			ps.setString(2, user);
			ps.setString(3, type);
			ps.setString(4, entityname);
			ps.setString(5, operation);
			ps.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(ps, connection);
		}
	}

	public String getRunlogInfo(String entityname) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String user = "";
		String sql = "select user from run_info where TimeStamp=(select MAX(TimeStamp) from run_info where entityname=?)";// SELECT__graylog_info
		try {
			connection = DBUtility.getConnection();
			ps = connection.prepareStatement(sql);
			ps.setString(1, entityname);
			rs = ps.executeQuery();
			if (rs.next()) {
				user = rs.getString(1);
			}
			System.out.println("user:" + user);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBUtility.releaseConnectionResources(rs, ps, connection);
		}
		return user;
	}

}
