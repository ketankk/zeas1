package com.taphius.databridge.scheduler;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taphius.databridge.model.DataSource;
import com.taphius.databridge.model.DataSourcerAttributes;
import com.itc.zeas.utility.DBUtility;
import com.taphius.datachecker.FileChecker;

public class RegisterSchedulerUtility {

	final static Logger logger = Logger
			.getLogger(RegisterSchedulerUtility.class);
	private FileChecker dataChecker;
	
	public static final String COMMA_STRING = ",";

	public FileChecker getDataChecker() {
		return dataChecker;
	}

	/**
	 * @param dataChecker
	 *            . To set fileChecker object
	 */
	public void setDataChecker(FileChecker dataChecker) {
		this.dataChecker = dataChecker;
	}

	public void registerScheduler(List<DataSourcerAttributes> newEntries) throws Exception {

		for (DataSourcerAttributes dsAttrs : newEntries) {
			try {
				String srcLocation = dsAttrs.getDataSrc().getLocation();
				if(srcLocation!=null){
				// Strip off last "/" character at the end of path
				if (srcLocation.endsWith("/")) {
					srcLocation = srcLocation.substring(0,
							srcLocation.length() - 1);
				}
				logger.info("Found Data Secheduler : " + dsAttrs.getName()
						+ " Checking File Format ---"
						+ dsAttrs.getDataSrc().getFormat());
				if (!dsAttrs.getDataSrc().getFormat().equalsIgnoreCase("mysql")) {
					if (!dataChecker.getIngestionMapping().containsKey(
							srcLocation)) {
						// register path
						Path toWatch = Paths.get(srcLocation);
						toWatch.register(dataChecker.getMyWatcher(),
								ENTRY_CREATE);
						logger.info("Going to register new path : "
								+ srcLocation);
						// dataChecker.getIngestionMapping().put(srcLocation,
						// DBUtility.getEntityId(dsAttrs.getName())+","+srcLocation+","+LoaderUtil.getBatchID(dsAttrs.getDataSet().getBatchStructure(),
						// dsAttrs.getFrequency())+","+dsAttrs.getDataSrc().getSchema()+","+dsAttrs.getDestinationDataset());
					}					
					
					StringBuilder infoBuilder = new StringBuilder();
					
					infoBuilder.append(DBUtility.getEntityId(dsAttrs.getName())).append(COMMA_STRING);
					infoBuilder.append(srcLocation).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getName()).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getDataSrc().getSchema()).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getDestinationDataset()).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getDataSet().getLocation()).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getFrequency()).append(COMMA_STRING);
					infoBuilder.append(dsAttrs.getDataSrc().getFormat()).append(COMMA_STRING);
					
					if (dsAttrs.getDataSrc().getFileData() != null) {
						infoBuilder.append(dsAttrs.getDataSrc().getFileData().getFileType()).append(COMMA_STRING);
					} else {
						infoBuilder.append("default").append(COMMA_STRING);
					}
					// added by Deepak to handle First record Header scenario start
					infoBuilder.append(dsAttrs.getDataSrc().getFileData().gethFlag());
					//flag indicates whether dataset needs to be encrypted
					//infoBuilder.append(dsAttrs.getDataSet().isEncrypted());
					logger.debug("Registered Ingstion profile with details -"+infoBuilder.toString());
					dataChecker.getIngestionMapping().put(srcLocation,
							infoBuilder.toString());
				}
				}
			} catch (IOException ioE) {
				logger.error(ioE.getMessage());
			}
		}
	}

	public static void main(String[] args) {
		String json = "{\"name\":\"test5_delimited\",\"type\":\"DataSchema\",\"dataAttribute\":[{\"Name\":\"NAME\",\"dataType\":\"string\"},{\"Name\":\"AGE\",\"dataType\":\"int\"},{\"Name\":\"DOB\",\"dataType\":\"string\",\"Confidential\":\"Obfuscate\"}],\"dataSchemaType\":\"Automatic\",\"fileData\":{\"fileName\":\"/home/zeas/data/delimited/test5.txt\",\"fileType\":\"Delimited\",\"rowDeli\":\"newline\",\"colDeli\":\"Control-A\"}}";
		Gson gSon = new GsonBuilder().create();
		// System.out.println(json);
		DataSource schema = gSon.fromJson(json, DataSource.class);

		System.out.println(schema.getFileData().getRowDeli());
		// System.out.println(json.substring(json.indexOf("\"colDeli\":\"")+11,
		// json.indexOf("\"", json.indexOf("\"colDeli\":\"")+11)));

	}

}
