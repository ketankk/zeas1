package com.zeas.spark.mlib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.zeas.spark.mlib.common.MlibCommon;
/**
 * This class is responsible for handing the Compare model scenario.
 * 
 * @author Nisith.Nanda
 *
 */
public class CompareModel {

	/**
	 * This Class is responsible to take the required common attributes and it's
	 * values from the provided Tested models eval.txt files and create single
	 * file.
	 * 
	 * @param args
	 *            argument list containing project component type
	 *            (Compare_Model), output directory paths of the tested models,
	 *            output location of evaluation / comparison file (input)
	 * 
	 * @return NA
	 */
	public void createComparisonFile(String[] args) {
		try {
			String[] filePaths = args[1].split(";");
			String outPutPath = args[2];

			// Instantiate list of different attributes of different ML
			// algorithms
			// (regression, classification etc.)
			List<String> binClassMetrics = Arrays.asList(MlibCommon.BINARY_CLASSIFICATION_METRICS.split(","));
			List<String> mulClassMetrics = Arrays.asList(MlibCommon.MULTI_CLASSIFICATION_METRICS.split(","));
			List<String> regMetrics = Arrays.asList(MlibCommon.REGRESSION_METRICS.split(","));
			List<String> clusterMetrics = Arrays.asList(MlibCommon.CLUSTERING_METRICS.split(","));
			Map<String, ArrayList<String>> propertyMap = new HashMap<String, ArrayList<String>>();
			FileSystem fs = FileSystem.get(new Configuration());

			// create map containing the ML attribute type and values by reading
			// the
			// evaluation metrics created by test model component
			for (int i = 0; i < filePaths.length; i++) {
				Path pt = new Path(filePaths[i] + MlibCommon.FILE_EXTN);
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] temp = line.split(":"); // @Nisith remove this hard
														// coding and put it as
														// delimiter in
														// constants

					String key = temp[0].trim();
					String value = null;

					// Check if the found key is among predefined attributes of
					// ML algorithms
					if (binClassMetrics.contains(key) || mulClassMetrics.contains(key) || regMetrics.contains(key)
							|| clusterMetrics.contains(key) || key.startsWith("Class")) {
						// get value of the given key (ML attribute)
						if (temp.length > 1) {
							value = temp[1].trim();
						}

						if (propertyMap.containsKey(key)) {
							propertyMap.get(key).add(value);
						} else {
							ArrayList<String> mlAttributeVal = new ArrayList<String>();
							mlAttributeVal.add(value);
							propertyMap.put(key, mlAttributeVal);
						}
					}
					line = br.readLine();
				}
				br.close();
			}

			List<String> travList = null;
			int numClass = 0;
			if (propertyMap.get("Type").get(0).equals(MlibCommon.REGRESSION)) {
				travList = regMetrics;
			} else if (propertyMap.get("Type").get(0).equals(MlibCommon.BINARY_CLASSIFICATION)) {
				travList = binClassMetrics;
			} else if (propertyMap.get("Type").get(0).equals(MlibCommon.MULTI_CLASSIFICATION)) {
				travList = mulClassMetrics;
				numClass = new Integer(propertyMap.get("Num Of Labels").get(0)).intValue();
			} else if (propertyMap.get("Type").get(0).equals(MlibCommon.CLUSTERING)) {
				travList = clusterMetrics;
			}
			Path pt = new Path(outPutPath + MlibCommon.FILE_EXTN);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

			for (String key : travList) {
				br.write(key + ":");
				for (int i = 0; i < propertyMap.get(key).size(); i++) {
					br.write(propertyMap.get(key).get(i));
					if (i + 1 < propertyMap.get(key).size()) {
						br.write(";");
					}
				}
				br.write("\n");
			}
			for (int j = 0; j < numClass; j++) {
				String key = "Class " + j + ".0 precision";
				br.write(key + ":");
				for (int i = 0; i < propertyMap.get(key).size(); i++) {
					br.write(propertyMap.get(key).get(i));
					if (i + 1 < propertyMap.get(key).size()) {
						br.write(";");
					}
				}
				br.write("\n");
				key = "Class " + j + ".0 recall";
				br.write(key + ":");
				for (int i = 0; i < propertyMap.get(key).size(); i++) {
					br.write(propertyMap.get(key).get(i));
					if (i + 1 < propertyMap.get(key).size()) {
						br.write(";");
					}
				}
				br.write("\n");
				key = "Class " + j + ".0 F1 score";
				br.write(key + ":");
				for (int i = 0; i < propertyMap.get(key).size(); i++) {
					br.write(propertyMap.get(key).get(i));
					if (i + 1 < propertyMap.get(key).size()) {
						br.write(";");
					}
				}
				br.write("\n");
			}
			br.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
