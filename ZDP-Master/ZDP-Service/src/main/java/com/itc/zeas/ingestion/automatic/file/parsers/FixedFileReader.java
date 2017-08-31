package com.itc.zeas.ingestion.automatic.file.parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.utility.filereader.FileReaderUtility;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import com.itc.zeas.profile.model.ExtendedDetails;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class FixedFileReader implements IFileDataTypeReader {

	static String fieldlen;
	static int NoofCols;
	String line;

	List<List<String>> colList = new ArrayList<>();
	boolean isFileWritten = false;
	String fileName;
	Logger logger = Logger.getLogger("FixedFileReader");

	/**
	 * Input to this method is file name and boolean isFileWritten. This Method
	 * will return Map containing
	 * 
	 */
	public Map<String, String> getColumnAndDataType(String fileName, boolean isFileWritten)
			throws ZeasFileNotFoundException {

		if (new File(fileName).exists() && new File(fileName).length() != 0) {
			this.isFileWritten = isFileWritten;
			this.fileName = fileName;

			logger.info("getColumnAndDataType method started");
			return getColumnAndDataType(fileName, null);

		} else {
			logger.warn("File does not exist or empty:" + fileName);
			colList = new ArrayList<List<String>>();
			logger.info("getColumnAndDataType method Ended");
			return new HashMap<>();
		}
	}

	/**
	 * Input to this method is file name. This Method will return Map containing
	 * Column name as key and respective Data types as values and write output
	 * file if file is not written.
	 * 
	 */
	@Override
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails)
			throws ZeasFileNotFoundException {

		NoofCols = dbDetails.getNoOfColumn();
		fieldlen = dbDetails.getFixedValues();

		System.out.println("file name :" + fileName);
		if (new File(fileName).exists() && new File(fileName).length() != 0) {
			colList = new ArrayList<List<String>>();
			Map<String, String> map = new LinkedHashMap<>();
			// String inputfile = fileName;
			System.out.println("******fileNmae :" + fileName);
			File file = new File(fileName);
			// File output = new File(inputfile.replace(".csv", "_Output.csv"));
			// writing file on local
			String inputfile = "";
			inputfile = fileName.replace("\\", "|");
			inputfile = fileName.replace("/", "|");
			int index = inputfile.indexOf(".");
			inputfile = inputfile.substring(0, index);
			inputfile = inputfile + FileReaderConstant.SAMPLE_FILE;
			System.out.println("### Input File :" + inputfile);
			// System.out.println("### APP DIR
			// :"+ConfigurationReader.getProperty("APP_DIR"));
			// inputfile = inputfile.replaceAll(".csv",
			// FileReaderConstant.SAMPLE_FILE);
			// File file = new File();
			String[] strArr = inputfile.split("\\|");
			String localPath = ConfigurationReader.getProperty("APP_DIR") + "/" + strArr[strArr.length - 1];

			// String localPath = strArr[strArr.length - 1];
			System.out.println("local path :**************:" + localPath);
			FileWriter writer = null;
			String[] header = null;
			try {
				if (!isFileWritten) {
					writer = new FileWriter(new File(localPath));
				}
				BufferedReader br = new BufferedReader(new FileReader(file));
				System.out.println("Fixed Length file :  file:" + file);

				String fielditem[] = fieldlen.split(",");
				int i, l, k, j;
				i = l = k = j = 0;
				line = br.readLine();
				if (line != null) {
					List<String> headList = new ArrayList<String>();
					for (String str : fielditem) {
						i = Integer.parseInt(str);
						String strout = line.toString().substring(l, i + j);
						headList.add(strout.trim());
						l = i + l;
						j = j + i;
					}
					header = headList.toArray(new String[headList.size()]);

				}
				int lineCount = 1;
				String line = br.readLine();
				if (!isFileWritten && line != null) {
					writer.write(line);
				}
				while (line != null && fieldlen != null) {
					i = l = k = j = 0;

					for (int colListSize = colList.size(); colListSize < fielditem.length; colListSize++) {
						colList.add(new ArrayList<String>());

					}
					for (String str : fielditem) {
						System.out.println(" i :" + i + "j : " + j + " k: " + k + " l: " + l);
						i = Integer.parseInt(str);
						System.out.println("Str :" + str);
						String strout = line.toString().substring(l, i + j);
						System.out.println("strout : " + strout);
						colList.get(k).add(strout.trim());

						l = i + l;
						j = j + i;
						k = k + 1;
					}

					line = br.readLine();
					if (!isFileWritten && line != null) {
						writer.write(System.lineSeparator());
						writer.write(line);
					}
					// To Read only only thousand lines
					++lineCount;

					if (lineCount > 1000)
						break;
				}

				br.close();
				if (!isFileWritten)
					writer.close();
				logger.info("Total nUmber of record read :" + lineCount);
			} catch (Exception e) {
				e.printStackTrace();
				// logger.error(e.toString());
				// String error=e.getMessage();
				// if(error.contains(":")){
				// error= error.substring(0, error.indexOf(":"));
				// }
				throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND, "error", "");
			}
			// put default column name and and respective datatypes
			int col = 0;
			System.out.println(colList);
			for (int i = 0; i < colList.size(); i++) {
				++col;
				if(dbDetails.gethFlag().equalsIgnoreCase("true")){
					try{
						System.out.println("header :"+header[i]);
					map.put(header[i], FileReaderUtility.columnDataType(colList.get(i)));
					}catch(Exception e){
						
					}
				}else{
				map.put(FileReaderConstant.DEFAULT_COLUMN + col,
						FileReaderUtility.columnDataType(colList.get(i)));
				try{
				colList.get(i).add(0,header[i]);
				}catch(Exception e){
					
				}
				}
			}
			logger.info("getColumnAndDataType method started");
			return map;
		} else {
			logger.warn("File Does Not Exist or empty " + fileName);
			logger.info("getColumnAndDataType method started");
			return new HashMap<String, String>();
		}
	}

	@Override
	// Return List of column values
	public List<List<String>> getColumnValues() {
		return colList;
	}

	public static void main(String args[]) {

		FixedFileReader dfr = new FixedFileReader();
		ExtendedDetails ext = new ExtendedDetails();
		ext.setNoOfColumn(3);
		ext.setFixedValues("10,3,10");
		ext.setFileName("D:\\TestData\\test2.txt");
		String fileName = "D:\\TestData\\test2.txt";

		try {
			dfr.getColumnAndDataType(fileName, ext);
		} catch (ZeasFileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
