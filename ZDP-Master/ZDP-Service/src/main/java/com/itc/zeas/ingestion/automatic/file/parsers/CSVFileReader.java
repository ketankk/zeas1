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

import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.utility.filereader.FileReaderUtility;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.utility.utility.ConfigurationReader;

public class CSVFileReader implements IFileDataTypeReader {

	Logger LOG=Logger.getLogger(CSVFileReader.class);
	String line;

	List<List<String>> colList = new ArrayList<>();
	boolean isFileWritten = false;
	private String fileName;


	/**
	 * Input to this method is file name and boolean isFileWritten. This Method
	 * will return Map containing
	 * 
	 */
	public Map<String, String> getColumnAndDataType(String fileName,
			boolean isFileWritten,ExtendedDetails extendedDetails) throws ZeasFileNotFoundException {
		
		if (new File(fileName).exists()
				&& new File(fileName).length()!=0) {
			this.isFileWritten = isFileWritten;
			this.fileName = fileName;

			LOG.info("getColumnAndDataType method started");
			return getColumnAndDataType(fileName, extendedDetails);

		} else {
			LOG.warn("File does not exist or empty:" + fileName);
			colList = new ArrayList<List<String>>();
			LOG.info("getColumnAndDataType method Ended");
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
		
		System.out.println("file name :"+fileName);
		if (new File(fileName).exists()
				&& new File(fileName).length()!=0) {
			colList = new ArrayList<List<String>>();
			Map<String, String> map = new LinkedHashMap<>();
			//String inputfile = fileName;
			System.out.println("******fileNmae :"+fileName);
			File file = new File(fileName);
			//writing file on local
			String inputfile="";
			 inputfile = fileName.replace("\\", "|");
			 inputfile = fileName.replace("/", "|");
			 inputfile = inputfile.replaceAll(".csv", FileReaderConstant.SAMPLE_FILE);
			//File file = new File();
			String[] strArr=inputfile.split("\\|");
			String localPath=ConfigurationReader.getProperty("APP_DIR")+"/"+strArr[strArr.length-1];
			LOG.info("local path :**************:"+localPath);
			FileWriter writer = null;
			String[] header = null;
			try {
			if(!isFileWritten){
				writer=new FileWriter(new File(localPath));
			}
			BufferedReader br = new BufferedReader(new FileReader(file));
			System.out.println("csv file :  file:"+file);
			line = br.readLine();
			if (line != null) {
				header = line.split(",");
			}
			int lineCount = 2;
			String line = br.readLine();
			if (!isFileWritten && line != null) {
				writer.write(line);
			}
			while (line != null) {

				String[] str = line.split(",",-1);// splitting the line based on
												// ","(comma) separation
				// Adding list inside main array list
				for (int i = colList.size(); i < str.length; i++) {
					colList.add(new ArrayList<String>());
				}
				// Adding data column wise in array list
				for (int i = 0; i < str.length; i++) {
					colList.get(i).add(str[i]);

				}

				line = br.readLine();
				if (!isFileWritten && line != null&& writer!=null) {
					writer.write(System.lineSeparator());
					writer.write(line);
				}
				// To Read only only thousand lines
				++lineCount;

				if (lineCount > 1000)
					break;
			}

			br.close();
			if(!isFileWritten && writer!=null)
			  writer.close();
				LOG.info("Total nUmber of record read :" + lineCount);
		}catch(Exception e){
				LOG.error(e.toString());
			String error=e.getMessage();
			if(error.contains(":")){
				error= error.substring(0, error.indexOf(":"));
			}
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,error,"");
		   }
			// put default column name and and respective datatypes
			int col = 0;
			for (int i = 0; i < colList.size(); i++) {
				++col;
				if(dbDetails.gethFlag().equalsIgnoreCase("true")){
					try{
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
			LOG.info("getColumnAndDataType method started");
			return map;
		} else {
			LOG.warn("File Does Not Exist or empty " + fileName);
			LOG.info("getColumnAndDataType method started");
			return new HashMap<String, String>();
		}
	}

	@Override
	// Return List of column values
	public List<List<String>> getColumnValues() {
		return colList;
	}

}
