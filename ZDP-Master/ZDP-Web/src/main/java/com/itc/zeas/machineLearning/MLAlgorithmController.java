package com.itc.zeas.machineLearning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.itc.zeas.machineLearning.model.MLAnalysis;
import com.itc.zeas.profile.EntityManager;
import com.itc.zeas.profile.model.Entity;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/rest/service")
public class MLAlgorithmController {

	@RequestMapping(value = "/runMLExecutor", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	String runMLExecutor(@RequestBody MLAnalysis mlanalysis) throws SQLException {

		MLExecutor ml = new MLExecutor();
		String results = null;
		try {
			results = ml.runMLAlroithm(mlanalysis);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
	}
	/**
	 * This method is used to get the completed processed pipelines
	 *
	 * @param String
	 * @return
	 * @return List<JSONObject>
	 * @throws IOException
	 */
	@RequestMapping(value = "/pipeline/runMachineLearning", method = RequestMethod.POST, headers = "Accept=application/json")
	public void runPipelineML(@RequestBody MLAnalysis mlAnalysis) throws IOException {
		EntityManager entityMngr = new EntityManager();

		String algorithm = mlAnalysis.getAlgorithm();
		Entity trainingEntity, testingEntity;
		try {
			trainingEntity = entityMngr.getEntityByName(mlAnalysis.getTraining().getDataSet());
			testingEntity = entityMngr.getEntityByName(mlAnalysis.getTesting().getDataSet());
			JSONObject trainingObject = new JSONObject(trainingEntity.getJsonblob());
			JSONObject testingObject = new JSONObject(testingEntity.getJsonblob());

			String trainingDataPath = trainingObject.getString("location");
			String testingDataPath = testingObject.getString("location");
			// Call Machine Learning Excecutor
			MachineLearningExecutor machineLearningExecutor = new MachineLearningExecutor();
			int accuracy = machineLearningExecutor.execute(algorithm, trainingDataPath, testingDataPath);

			// Insert Machine Learning results to DB
			mlAnalysis.setAccuracy(accuracy);
			entityMngr.addMLAnalysis(mlAnalysis);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	/**
	 * This method is used to get all machine Learning analysis pipelines
	 *
	 * @return
	 * @return List<JSONObject>
	 * @throws IOException
	 */
	@RequestMapping(value = "/pipeline/getMLAnalysis", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody
	List<MLAnalysis> getMLAnalysis() throws IOException {
		EntityManager entityMngr = new EntityManager();
		List<MLAnalysis> mlAnalysisList = new ArrayList<MLAnalysis>();
		try {
			mlAnalysisList = entityMngr.getMLAnalysis();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mlAnalysisList;

	}



	/**
	 * this method is to delete a machine Learning analysis
	 *
	 * @param entityId
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "/pipeline/deleteMLAnalysis/{mlID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody void deleteMLAnalysis(@PathVariable("mlID") Integer mlID) throws IOException {
		EntityManager entityManager = new EntityManager();
		try {
			entityManager.deleteEntity(mlID);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
