package com.itc.zeas.ingestion.automatic.file.parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.itc.zeas.utility.filereader.FileReaderConstant;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;

import com.itc.zeas.utility.utility.ConfigurationReader;

public class JSONParser extends Thread {

	Logger logger = Logger.getLogger("JSONParser");
	private String fileName;
	private boolean isSmallFile;
	private Map<String, List<String>> colNameAndValues;

	public JSONParser(String fileName, boolean isSmallFile) {
		this.fileName = fileName;
		this.isSmallFile = isSmallFile;
		colNameAndValues = new LinkedHashMap<>();
	}

	public void run() {

		if (isSmallFile)
			// this will execute for writing sample file thread
			readSampleJsonFile();
		else {
			// this will execute for writing full file thread
			// WriteJSONToCSVJsonFile();
		}

	}

	/*
	 * returns column name and values.
	 */
	public Map<String, List<String>> getcolNameAndValues() {

		return colNameAndValues;
	}

	/*
	 * reading sample file for preview and writing sample csv for test run.
	 */
	private void readSampleJsonFile() {

		logger.info("start readSampleJsonFile method thread name:" + Thread.currentThread().getName());
		boolean isCSVWriteStarted = false;
		JsonParser jp = null;
		try {
			FileWriter fileWriter = null;
			JsonFactory f = new MappingJsonFactory();
			String newFileName = fileName.replaceAll(".json", FileReaderConstant.SAMPLE_FILE);
			logger.info("file name:::::::" + newFileName);
			System.out.println("fileName:" + newFileName);
			// writing file on local
			// test
			newFileName = newFileName.replace("\\", "|");
			newFileName = newFileName.replace("/", "|");
			// File file = new File();
			String[] smallArr = newFileName.split("\\|");
			newFileName = ConfigurationReader.getProperty("APP_DIR") + "/" + smallArr[smallArr.length - 1];
			System.out.println("local path :*******:" + newFileName);
			logger.info("local path -----:**********:" + newFileName);

			File jsonFile = new File(fileName);
			InputStream inputStream = new FileInputStream(jsonFile);
			int content;
			StringBuilder stringBuilder = new StringBuilder();
			while ((content = inputStream.read()) != -1) {
				stringBuilder.append((char) content);
			}
			/* Data is reading from file. */
			if(stringBuilder.toString().length() < 5){
				return;
			}
			jp = f.createJsonParser(stringBuilder.toString());
			JsonToken current;
			current = jp.nextToken();
			if (current != JsonToken.START_OBJECT) {
				System.out.println("Error: root should be object: quiting.");
				return;
			}

			int lineCount = 0;
			while (jp.nextToken() != JsonToken.END_OBJECT) {
				// System.out.println("*********************************************");
				current = jp.nextToken();
				/* Need to escape the checking of start array */
				if (!(current == JsonToken.START_ARRAY)) {
					// For each of the records in the array no need tto check jp
					// as null
					while (jp.nextToken() != JsonToken.END_ARRAY) {
						// read the record into a tree model,
						// this moves the parsing position to the end of it
						JsonNode node = jp.readValueAsTree();
						// System.out.println("##########################################");

						lineCount++;
						// And now we have random access to everything in the
						// object
						Iterator<String> fieldNames = node.getFieldNames();
						while (fieldNames.hasNext()) {

							String fName = fieldNames.next();
							Iterator<Entry<String, JsonNode>> subNodes = node.get(fName).getFields();
							boolean isSubArray = false;
							while (subNodes.hasNext()) {
								isSubArray = true;
								Entry<String, JsonNode> subNode = subNodes.next();
								String keyName = fName + "_" + subNode.getKey();
								String value = subNode.getValue().toString();
								value = value.replaceAll(",", " ");
								value = value.replaceAll("\"", "");
								keyName = keyName.replaceAll(" ", "_");
								if (colNameAndValues.containsKey(keyName)) {
									colNameAndValues.get(keyName).add(value);
								} else {
									colNameAndValues.put(keyName, new ArrayList<String>());
									colNameAndValues.get(keyName).add(value);
								}
							}

							if (!isSubArray) {

								String value = "";
								String keyName = fName.trim();
								keyName = keyName.replaceAll(" ", "_");
								if (node.get(fName).isTextual()) {
									value = node.get(fName).getTextValue();
								} else if (node.get(fName).isInt()) {
									Integer ival = node.get(fName).getIntValue();
									value = ival.toString();
								} else if (node.get(fName).isDouble()) {
									Double dval = node.get(fName).getDoubleValue();
									value = dval.toString();
								} else if (node.get(fName).isLong()) {
									Long lval = node.get(fName).getLongValue();
									value = lval.toString();
								}
								value = value.replaceAll(",", " ");
								if (colNameAndValues.containsKey(keyName)) {
									colNameAndValues.get(keyName).add(value);
								} else {
									colNameAndValues.put(keyName, new ArrayList<String>());
									colNameAndValues.get(keyName).add(value);
								}

							}

						}
						if (lineCount == 1000) {

							jp.close();
							jp = null;
							break;
						}

					}

					// else{
					// System.out.println("local path :in*******:"+newFileName);
					logger.info("local path -----:in**********:" + newFileName);
					StringBuilder lines = getDataAsString(colNameAndValues);
					if (!(lines.length() == 0)) {
						fileWriter = new FileWriter(new File(newFileName));
						fileWriter.write(lines.toString());
						fileWriter.flush();
						fileWriter.close();
					}
					logger.info("successfully parse file:" + newFileName);
					// }

				} else {
					System.out.println("Error: records should be an array: skipping.");
					jp.skipChildren();
				}
			}
		} catch (Exception e) {
			logger.error("gets error during parsing json file:" + e.toString());
			try {
				if (jp != null)
					jp.close();
			} catch (IOException e1) {
				logger.error("close json file:" + e.toString());
			}
		}
	}

	/*
	 * public static void main(String[] args) throws JsonParseException,
	 * IOException { JSONParser jsonParser = new
	 * JSONParser("D:\\Files\\testfile\\Sample.json", false);
	 * jsonParser.readSampleJsonFile(); Map<String, List<String>>
	 * getcolNameAndValues = jsonParser.getcolNameAndValues(); //Set<String>
	 * keySet = getcolNameAndValues.keySet(); for(Map.Entry<String,
	 * List<String>> entry : getcolNameAndValues.entrySet()){
	 * System.out.println(entry.getKey()+" "+entry.getValue().toString()); }
	 * JsonFactory jasonFactory = new JsonFactory(); JsonParser jsonParser =
	 * jasonFactory.createJsonParser(new
	 * File("D:\\Files\\testfile\\Sample.json"));
	 * System.out.println(jsonParser.getText());
	 * System.out.println(jsonParser.getTextLength()); while
	 * (jsonParser.nextToken() != JsonToken.END_OBJECT) { String fieldname =
	 * jsonParser.getCurrentName();
	 * 
	 * if ("id".equals(fieldname)) { //move to next token
	 * jsonParser.nextToken(); System.out.println(jsonParser.getText()); }
	 * 
	 * if("name".equals(fieldname)){ //move to next token
	 * jsonParser.nextToken(); System.out.println(jsonParser.getText()); } } }
	 */
	/*
	 * reading full json file and writing csv for for ingestion.
	 */
	private void WriteJSONToCSVJsonFile() {

		logger.info("start WriteJSONToCSVJsonFile method : thread name" + Thread.currentThread().getName());
		boolean isCSVWriteStarted = false;
		JsonParser jp = null;
		FileWriter fileWriter = null;
		String newFileName = null;
		try {
			JsonFactory f = new MappingJsonFactory();
			newFileName = fileName.replaceAll(".json", ".csv");
			logger.info("file name:::::::" + newFileName);
			System.out.println("fileName:" + newFileName);
			// writing file on local
			// test
			newFileName = newFileName.replace("\\", "|");
			newFileName = newFileName.replace("/", "|");
			// File file = new File();
			String[] smallArr = newFileName.split("\\|");
			newFileName = ConfigurationReader.getProperty("APP_DIR") + "/" + smallArr[smallArr.length - 1];
			System.out.println("local path :*******:" + newFileName);
			logger.info("local path -----:**********:" + newFileName);
			// end
			jp = f.createJsonParser(new File(fileName));
			JsonToken current;
			current = jp.nextToken();
			if (current != JsonToken.START_OBJECT) {
				System.out.println("Error: root should be object: quiting.");
				return;
			}

			while (jp != null && jp.nextToken() != JsonToken.END_OBJECT) {
				current = jp.nextToken();
				if (current == JsonToken.START_ARRAY) {
					// For each of the records in the array
					while (jp != null && jp.nextToken() != JsonToken.END_ARRAY) {
						// read the record into a tree model,
						// this moves the parsing position to the end of it
						JsonNode node = jp.readValueAsTree();
						// And now we have random access to everything in the
						// object
						Iterator<String> fieldNames = node.getFieldNames();
						while (fieldNames.hasNext()) {

							String fName = fieldNames.next();
							Iterator<Entry<String, JsonNode>> subNodes = node.get(fName).getFields();
							boolean isSubArray = false;
							while (subNodes.hasNext()) {
								isSubArray = true;
								Entry<String, JsonNode> subNode = subNodes.next();
								String keyName = fName + "_" + subNode.getKey();
								String value = subNode.getValue().toString();
								value = value.replaceAll(",", " ");
								value = value.replaceAll("\"", "");
								keyName = keyName.replaceAll(" ", "_");
								if (colNameAndValues.containsKey(keyName)) {
									colNameAndValues.get(keyName).add(value);
								} else {
									colNameAndValues.put(keyName, new ArrayList<String>());
									colNameAndValues.get(keyName).add(value);
								}
								// System.out.println(fName + "_" +
								// subNode.getKey()+ ": " +
								// subNode.getValue().asText());
							}

							if (!isSubArray) {

								String value = "";
								String keyName = fName.trim();
								keyName = keyName.replaceAll(" ", "_");
								if (node.get(fName).isTextual()) {
									value = node.get(fName).getTextValue();
								} else if (node.get(fName).isInt()) {
									Integer ival = node.get(fName).getIntValue();
									value = ival.toString();
								} else if (node.get(fName).isDouble()) {
									Double dval = node.get(fName).getDoubleValue();
									value = dval.toString();
								} else if (node.get(fName).isLong()) {
									Long lval = node.get(fName).getLongValue();
									value = lval.toString();
								}
								value = value.replaceAll(",", " ");
								if (colNameAndValues.containsKey(keyName)) {
									colNameAndValues.get(keyName).add(value);
								} else {
									colNameAndValues.put(keyName, new ArrayList<String>());
									colNameAndValues.get(keyName).add(value);
								}

							}

						}
						// if(lineCount<=1000){

						if (colNameAndValues.size() > 0) {
							Set<String> keys = colNameAndValues.keySet();
							String tempKey = "";
							for (String key : keys) {
								tempKey = key;
								break;
							}
							int columnSize = colNameAndValues.get(tempKey).size();
							if (columnSize == 500) {
								if (!isCSVWriteStarted) {
									fileWriter = new FileWriter(new File(newFileName));
									isCSVWriteStarted = true;
								}
								StringBuilder tempLines = getDataAsString(colNameAndValues);
								fileWriter.write(tempLines.toString());
								colNameAndValues = new LinkedHashMap<>();
							}
						}

					}
					if (isCSVWriteStarted) {
						StringBuilder lines = getDataAsString(colNameAndValues);
						if (!(lines.length() == 0)) {
							fileWriter.write(lines.toString());
						}
						fileWriter.close();
						logger.info("file successfully written" + newFileName);
					} else {

						StringBuilder lines = getDataAsString(colNameAndValues);
						if (!(lines.length() == 0)) {
							fileWriter = new FileWriter(new File(newFileName));
							fileWriter.write(lines.toString());
							fileWriter.close();
							logger.info("file successfully written" + newFileName);
						}
					}

				} else {
					System.out.println("Error: records should be an array: skipping.");
					jp.skipChildren();
				}

			}
			// closing the jason parser
			jp.close();
		} catch (Exception e) {
			logger.error("fileis not able to parse so might be lead ingestion faliure****************");
			logger.error("error during parsing the json file **:\n  " + e.toString());
			try {
				if (fileWriter != null)
					fileWriter.close();
			} catch (IOException e1) {
				logger.error("error :" + e1.toString());
			}
			// delete the empty file if gets parse fail
			(new File(newFileName)).delete();
		}
		// added finally block to close open resources , like filewriter...
		finally {
			try {
				if (fileWriter != null)
					fileWriter.close();
			} catch (IOException e1) {
				logger.error("error :" + e1.toString());
			}
		}

	}

	/**
	 * prepare stringbuilder to write content into file
	 * 
	 */
	private StringBuilder getDataAsString(Map<String, List<String>> colAndValues) {

		StringBuilder strBuilder = new StringBuilder("");
		// find the highest length of columns
		int max = 0;
		Set<String> keys = colAndValues.keySet();
		for (String key : keys) {
			int temp = colAndValues.get(key).size();
			if (temp > max) {
				max = temp;
			}
		}

		int colSize = colAndValues.size();
		// prepare the string builder.
		for (int i = 0; i < max; i++) {
			StringBuilder tempStr = new StringBuilder("");
			int count = 1;
			for (String key : keys) {
				String strVal = "";
				try {
					strVal = colAndValues.get(key).get(i);
				} catch (Exception e) {

				}
				tempStr.append(strVal);
				if (count < colSize) {
					tempStr.append(",");
				}
				count++;
			}
			tempStr.append("\n");
			strBuilder.append(tempStr);
		}

		return strBuilder;
	}
}
