package com.itc.zeas.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.itc.zeas.utility.filereader.FileReaderUtility;
import com.itc.zeas.project.model.NameAndDataType;
import org.apache.log4j.Logger;

import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.ingestion.model.ZDPRunLogDetails;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class FileUtility {

	private static final Logger LOGGER = Logger.getLogger(FileUtility.class);

	public static List<String> getLogsContents(String appName, String pageNo) {

		List<String> lines = new ArrayList<>();
		List<String> values = new ArrayList<>();
		String path = "";
		// TODO config properties should be read from centralized place
		// InputStream inputStream = null;
		// String databridgeLogPath = "";
		// String webModuleLogPath = "";
		// Properties prop = new Properties();
		// try {
		// inputStream = new
		// FileInputStream(System.getProperty("user.home")+"/zeas/Config/config.properties");
		// prop.load(inputStream);
		// databridgeLogPath=prop.getProperty("databridge_log_path");
		// webModuleLogPath=prop.getProperty("webmodule_log_path");
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// finally{
		// if(inputStream !=null){
		// try {
		// inputStream.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// }
		String databridgeLogPath = ConfigurationReader
				.getProperty("databridge_log_path");
		String webModuleLogPath = ConfigurationReader
				.getProperty("webmodule_log_path");
		LOGGER.debug("VALUE FROM CONFIG PROPERTY-- databridgeLogPath: "
				+ databridgeLogPath + "webModuleLogPath" + webModuleLogPath);
		if ((appName != null && !appName.isEmpty())
				&& (pageNo != null && !pageNo.isEmpty())) {
			Integer pageCount = 0;
			try {
				pageCount = Integer.parseInt(pageNo);
			} catch (NumberFormatException e) {

			}
			if (appName.equalsIgnoreCase("databridge")) {
				path = databridgeLogPath;
			} else if (appName.equalsIgnoreCase("webmodule")) {
				path = webModuleLogPath;
			}
			if (pageCount > 0 && !path.isEmpty()) {
				String[] args1 = new String[4];
				args1[0] = "/bin/bash";
				args1[1] = System.getProperty("user.home")
						+ "/zeas/Config/logreader.sh";
				args1[2] = path;
				args1[3] = pageNo;
			//	lines = runScript(args1);
			}
		}

		return lines;
	}

	private static List<String> runScript(String... args) {

		ProcessBuilder pb = new ProcessBuilder(args);
		List<String> lines = new ArrayList<>();
		pb.redirectErrorStream(true);
		pb.redirectErrorStream(true);
		Process p = null;
		BufferedReader br = null;
		try {
			p = pb.start();
			br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			p.waitFor();
			// System.out.println("br output =="+br.toString() +
			// "=="+br.readLine());
			while (br.ready()) {
				String str = br.readLine().trim();
				lines.add(0, str);
				// System.out.println("str=="+str);
			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (p != null) {
				p.destroy();
			}
		}

		return lines;
	}

	// append log contents into existing log file.
	public static void runLogAppend(String dirPath, String fileName,
			String strContents) {

		FileWriter fileWriter = null;
		BufferedWriter bf = null;
		// String fileName = dir;
		File file = new File(dirPath);
		try {
			if (!file.isDirectory()) {
				System.out.println("dir");
				file.mkdirs();
			}

			File file11 = new File(dirPath + "/" + fileName);
			fileWriter = new FileWriter(file11, true);
			bf = new BufferedWriter(fileWriter);
			// fileWriter = new FileWriter(new File(fileName));
			bf.write(strContents);
			bf.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (bf != null) {
				try {
					bf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static List<String> getLogs(String type, String name,
			String version, String userName, Integer noOfRecordSkipped) {

		ZDPDataAccessObjectImpl accessObjectImpl = new ZDPDataAccessObjectImpl();
		ZDPRunLogDetails details = null;
		List<String> strLogs = new ArrayList<>();
		BufferedReader reader=null;
		try {
			details = accessObjectImpl.getLatestRunLogDetail(name, userName);
			String filePath = details.getLogfilelocation();
			String fileName = filePath + "/log.txt";
			reader = new BufferedReader(new FileReader(fileName));
			String line;
			int c = 1;
			while ((line = reader.readLine()) != null) {
				if (c > noOfRecordSkipped)
					strLogs.add(line);
				c++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(reader !=null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return strLogs;
	}

	public static List<NameAndDataType> getSchema(File schemaFile) {

		List<NameAndDataType> schema = new ArrayList<NameAndDataType>();
		BufferedReader buffReader = null;
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(schemaFile);
			buffReader = new BufferedReader(fileReader);
			String line = buffReader.readLine();
			while (line != null) {
				NameAndDataType nameAndDataType = new NameAndDataType();

				if (line.contains("|")) {
					String nameAndDatatype[] = line.split("\\|");
					if (!nameAndDatatype[0].replaceAll("[^\\p{Alpha}]+", "").equalsIgnoreCase("fieldname")) {
						nameAndDataType.setName(nameAndDatatype[0].trim().replaceAll("[^0-9\\p{Alpha}]+", "_"));
						nameAndDataType.setDataType(FileReaderUtility.getActualDataType(nameAndDatatype[1].trim()));
						schema.add(nameAndDataType);
					}
				} else {
				}
				line = buffReader.readLine();
			}
		} catch (IllegalStateException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileReader != null)
					fileReader.close();
				if (buffReader != null)
					buffReader.close();
			} catch (Exception e2) {
				// Ignore
			}
		}
		return schema;
	}
}
