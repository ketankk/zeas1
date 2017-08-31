package com.itc.zeas.filereader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

import org.json.JSONObject;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.model.Entity;
import com.itc.taphius.utility.ConfigurationReader;
/**
 * 
 * @author 19173 Getting HDFS location of the source file from with Dataset name
 *
 */
public class ExportDataSet {

	public String gethdfspath(String s) throws ClassNotFoundException,
	SQLException {		
		EntityManager em = new EntityManager();
		Entity e = em.getEntityByName(s);

		JSONObject jObject = new JSONObject(e.getJsonblob());

		String hdfspath = jObject.getString("location");
		System.out.println(hdfspath);
		return hdfspath;

	}
	
	/**
	 * 
	 * @param dset - parameter to pass input file
	 * @param resultpath - parameter to pass output location to store destination file
	 */
	public void exportDataset(String dset,String resultpath)  {
		String str = null;
		String dataset = dset;
		ExportDataSet ex = new ExportDataSet();
		
		try {
			String res = ex.gethdfspath(dataset);
			ProcessBuilder pb = null;

		
			pb = new ProcessBuilder("/bin/bash", System.getProperty("user.home")+ConfigurationReader.getProperty("HDFS_DATASET_EXPORT_DIR")+"/hdfsexport.sh", res,
					resultpath);


			// Start the process.

			if (res != null) {
				Process p = pb.start();
				p.waitFor();
				BufferedReader stdInput = new BufferedReader(
						new InputStreamReader(p.getInputStream()));

				BufferedReader stdError = new BufferedReader(
						new InputStreamReader(p.getErrorStream()));

				// read the output from the command
				System.out.println("Here is the standard output of the command:\n");
				while ((str = stdInput.readLine()) != null) {
					System.out.println(str);
				}

				// read any errors from the attempted command
				// System.out.println("Here is the standard error of the command (if any):\n");
				while ((str = stdError.readLine()) != null) {
					System.out.println(str);
				}
			} else {
				System.out.println("entered HDFS file not exist");

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
