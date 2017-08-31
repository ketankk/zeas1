package com.itc.zeas.filereader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.*; 

import com.itc.taphius.utility.ConfigurationReader;
import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasFileNotFoundException;
/**
 * 
 * Description: file reader utility for various column and row types delimited files.
 * @author: 18972
 * 
 * Version: 
 * 
 * Date: 21-July-2015
 */
public class DelimitedFilesReader implements IFileDataTypeReader {
	// delimiters types
	static String column_delim = "";
	static String row_delim = "";
	static String column_delimterused = "";
	static String row_delimterused = "";

    String line;
	List<List<String>> colList = new ArrayList<>();
	boolean isFileWritten = false;
	String fileName;

	// Logger logger = Logger.getLogger("DelimitedFilesReader");

	/**
	 * Input to this method is file name and boolean isFileWritten. This Method
	 * will return Map containing
	 * 
	 */
	public Map<String, String> getColumnAndDataType(String fileName,
			boolean isFileWritten) throws ZeasFileNotFoundException {

		if (new File(fileName).exists() && new File(fileName).length() != 0) {
			this.isFileWritten = isFileWritten;
			this.fileName = fileName;

			// logger.info("getColumnAndDataType method started");
			return getColumnAndDataType(fileName, null);

		} else {
			// logger.warn("File does not exist or empty:" + fileName);
			colList = new ArrayList<List<String>>();
			// logger.info("getColumnAndDataType method Ended");
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
	public Map<String, String> getColumnAndDataType(String fileName,
			ExtendedDetails dbDetails) throws ZeasFileNotFoundException {
		Map<String, String> delimiters = new HashMap();
		delimiters.put("tab", "\t");
		delimiters.put("space", " ");
		delimiters.put("comma", ",");
		delimiters.put("underscore", "_");
		delimiters.put("slash", "-");
		delimiters.put("Control-A", "\\^A");
		delimiters.put("Control-B", "\\^B");
		delimiters.put("Control-C", "\\^C");
		delimiters.put("newline", "\n");
		delimiters.put("carriage return", "\r\n");
		

		column_delim = dbDetails.getColDeli();
		row_delim = dbDetails.getRowDeli();
		column_delimterused = delimiters.get(column_delim);
		row_delimterused =delimiters.get(row_delim);
		
		
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
			String sampleFile = inputfile + ".SAMPLE_FILE";
			// inputfile = inputfile.replaceAll(".csv",
			// FileReaderConstant.SAMPLE_FILE);
			// File file = new File();
			String[] strArr = inputfile.split("\\|");
			/*String localPath = ConfigurationReader.getProperty("APP_DIR") + "/"
					+ strArr[strArr.length - 1];*/
			String localPath = strArr[strArr.length - 1];
			System.out.println("local path :**************:" + localPath);
			FileWriter writer = null;
			String[] header = null;
			try {
				if (!isFileWritten) {
					writer = new FileWriter(new File(sampleFile));
				}
				Scanner scanner = new Scanner(file);
				System.out.println("delimited_separated file :  file:" + file);
				row_delimterused = dbDetails.getRowDeli();
				scanner.useDelimiter(row_delimterused);
				String line=scanner.nextLine();
				if (line != null) {
					header = line.split(column_delimterused);
					// for (int i = 0; i < header.length; i++) {
					// header[i] = header[i].replaceAll("[^a-zA-Z0-9 ]",
					// "").trim();
					// header[i] = header[i].replaceAll("[ ]+", "_");
					// }
				}
				
			    line =scanner.nextLine();
				if (!isFileWritten && line != null) {
					writer.write(line);
				}
				
				int lineCount = 2;
				while (scanner.hasNext()) {
					String[] str = line.split(column_delimterused);// splitting the
																// line based on
					// delimiter separation
					// Adding list inside main array list
					for (int colListSize = colList.size(); colListSize < str.length; colListSize++) {
						colList.add(new ArrayList<String>());
					}
					// Adding data column wise in array list
					for (int i = 0; i < str.length; i++) {
						colList.get(i).add(str[i]);

					}

					line =scanner.nextLine();
					if (!isFileWritten && line != null) {
						writer.write(System.lineSeparator());
						writer.write(line);
					}
					// To Read only only thousand lines
					++lineCount;

					if (lineCount > 1000)
						break;
				}

				scanner.close();
				if (!isFileWritten)
					writer.close();
				// logger.info("Total nUmber of record read :" + lineCount);
			} catch (Exception e) {
				// logger.error(e.toString());
				e.printStackTrace();
				String error = e.getMessage();
System.out.println(" --------- "+ error);
				if (null !=error && error.contains(":")) {
					error = error.substring(0, error.indexOf(":"));
				}
				throw new ZeasFileNotFoundException(
						ZeasErrorCode.FILE_NOT_FOUND, error, "");
			}
			// put default column name and and respective datatypes
			int col = 0;
			for (int colListSize = 0; colListSize < colList.size(); colListSize++) {
				++col;
				map.put(FileReaderConstant.DEFAULT_COLUMN + col,
						FileReaderUtility.columnDataType(colList
								.get(colListSize)));
			}
			// logger.info("getColumnAndDataType method started");
			return FileReaderUtility.getColumnNameAndDataType(header, map);
		} else {
			// logger.warn("File Does Not Exist or empty " + fileName);
			// logger.info("getColumnAndDataType method started");
			return new HashMap<String, String>();
		}
	}

	@Override
	// Return List of column values
	public List<List<String>> getColumnValues() {
		return colList;
	}

	public static void main(String args[]) {

		DelimitedFilesReader dfr = new DelimitedFilesReader();
		ExtendedDetails ext = new ExtendedDetails();
		ext.setColDeli("tab");
		ext.setRowDeli("newline");
		ext.setFileName("D:\\TestData\\test1.txt");
		String fileName = ext.getFileName();
		try {
			dfr.getColumnAndDataType(fileName, ext);
		} catch (ZeasFileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
