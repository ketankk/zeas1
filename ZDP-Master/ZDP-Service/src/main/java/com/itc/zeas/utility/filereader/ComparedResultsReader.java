package com.itc.zeas.utility.filereader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class ComparedResultsReader {

	Logger logger = Logger.getLogger("ComparedResultsReader");
	public static final String DATA_DELIM = "-";

	/**
	 * Method for getting the Comparison results from the HDFS and returing back
	 * to UI for display
	 * 
	 * @param details
	 * @throws ZeasException
	 */
	public Map<String, List<List<String>>> getResults(String details) throws ZeasException {
		String[] detList = details.split(",", -1);

		if (detList.length != 3) {
			throw new ZeasException(0, "Insufficent Arguments. Expected 3 but got " + detList.length, null);
		}

		String filePath = detList[2] + "_eval.txt";
		Map<String, List<List<String>>> results = new LinkedHashMap<String, List<List<String>>>();
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
			FileSystem fs = FileSystem.get(conf);
			Path pt = new Path(filePath);

			// File fs = new File(filePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			String line;
			line = br.readLine();
			int i = 0;
			while (line != null) {
				String[] temp = line.split(":");
				String[] vals = temp[1].split(";");
				List<List<String>> valList = new ArrayList<List<String>>();
				
				for (int k = 0; k < vals.length; k++) {
					
					List<String> val = new ArrayList<String>();
					if (vals[k].trim().split(DATA_DELIM).length > 1) {
						String[] subVals = vals[k].trim().split(DATA_DELIM);
						{
							for (String subVal : subVals) {
								val.add(subVal);

							}
						}
					} else {
						val.add(vals[k].trim());
					}
					valList.add(val);
				}
				results.put(temp[0].trim(), valList);
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ZeasException(0, "Error Reading the Compared Result File " + filePath, e.getMessage());
		}
		return results;
	}

}
