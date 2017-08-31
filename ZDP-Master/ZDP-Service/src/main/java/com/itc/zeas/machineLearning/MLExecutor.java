package com.itc.zeas.machineLearning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.itc.zeas.profile.EntityManager;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.itc.zeas.machineLearning.model.MLAnalysis;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class MLExecutor {
	public String runMLAlroithm(MLAnalysis ml) throws Exception {

		EntityManager em = new EntityManager();
		String datasetPath = em.getdatasetPath(ml.getTraining().getDataSet());

		int labelIndex = ml.getSchema().indexOf(ml.getLabel());
		String featureIndex = "";
		String features[] = ml.getFeatures().split(",");
		for (String feature : features) {
			featureIndex += ml.getSchema().indexOf(feature) + ",";
		}
		featureIndex = featureIndex.substring(0, featureIndex.length());
		System.out.println("Feature index string - " + featureIndex);

		String[] args = new String[7];
		args[0] = ShellScriptExecutor.BASH;
		args[1] = System.getProperty("user.home")
				+ "/zeas/Config/MLAlgorithm.sh";
		args[2] = ml.getAlgorithm().replaceAll(" ", "");
		args[3] = ConfigurationReader.getProperty("NAMENODE_HOST")
				+ datasetPath + File.separator + "cleansed";
		args[4] = ml.getSize();
		args[5] = Integer.toString(labelIndex);
		args[6] = featureIndex;

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		shExe.runScript(args);
		String resultPath = "/tmp/" + args[3].substring(25);
		System.out.println("result path is ------------------------"
				+ resultPath);

		// String results ="";
		StringBuffer results = new StringBuffer();
		try {
			// results = new String(Files.readAllBytes(Paths.get(resultPath)));
			File file = new File(resultPath);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				results.append(line);
				results.append("|");
			}
			fileReader.close();
			System.out.println("Contents of file:");
			System.out.println(results.toString());

		} catch (IOException e) {

			e.printStackTrace();
		}
		String finalResults = results.toString();
		System.out.println(finalResults);

		return finalResults;

	}

}
