package com.itc.zeas.utility;

import java.io.IOException;
import java.sql.SQLException;

import com.itc.zeas.project.model.ProjectEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.taphius.databridge.utility.ShellScriptExecutor;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class ExportControllerUtility {
	/**
	 * 
	 * @param datasetPath
	 * @param datasetPath
	 * @return
	 */
	public Entity populateEntityLocationAndDatasetName(String datasetPath) {
		Entity entity = new Entity();
		entity.setLocation(ConfigurationReader.getProperty("FILE_EXPORT_PATH"));
		entity.setName(datasetPath);
		return entity;
	}

	/**
	 * 
	 * @param entity
	 * @param format
	 * @return boolean value Method perform the export operation & return back
	 *         if export is success
	 */
	public boolean isFileExported(Entity entity, String format) {
		String[] args = new String[4];
		args[0] = ShellScriptExecutor.BASH;
		/* I added check for if request URL having empty parameter */
		if (format == null || entity.getLocation().length() < 1 || entity.getName().length() < 1) {
			return false;
		}
		/* Below logic check the format of file which need to export */
		if (format.equalsIgnoreCase("csv")) {
			args[1] = System.getProperty("user.home") + "/zeas/Config/exportcsv.sh";
		} else if (format.equalsIgnoreCase("json")) {
			args[1] = System.getProperty("user.home") + "/zeas/Config/exportjson.sh";
		} else {
			args[1] = System.getProperty("user.home") + "/zeas/Config/exportHiveView.sh";
		}
		/*Changes as per discussion that _dataset will be handle by UI*/
		args[2] = entity.getName();
		args[3] = entity.getLocation();

		ShellScriptExecutor shExe = new ShellScriptExecutor();
		int status = shExe.runScript(args);
		return (status == 0) ? true : false;
	}
	
	 public String getTableName(final String entityId) throws ZeasException {
		ZDPDataAccessObjectImpl zdpDataAccessObjectImpl = new ZDPDataAccessObjectImpl();
		ProjectEntity projectEntityDetails;
		try {
			projectEntityDetails = zdpDataAccessObjectImpl.getEntityDetails(entityId);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(projectEntityDetails.getJsonblob());
			return rootNode.path("name").getTextValue();
		} catch (SQLException | IOException e) {
			throw new ZeasException(100,"Table Doesn't Exist ","");
		}
	}
	
	public String buildHiveQuery(String tableName, String hiveQuery) {
		//regex to replace table1,Table2,taBle33 type string from query
		String regex="(?i)table+[\\d]*";
				hiveQuery=hiveQuery.replaceAll(regex, tableName+"_dataset");
		
		return hiveQuery;
	}
}
