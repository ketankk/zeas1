package com.itc.zeas.filereader;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.itc.taphius.utility.ConfigurationReader;
import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasFileNotFoundException;

public class FixedFileReader implements IFileDataTypeReader{

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
	public Map<String, String> getColumnAndDataType(String fileName,
			boolean isFileWritten) throws ZeasFileNotFoundException {

		if (new File(fileName).exists()
				&& new File(fileName).length()!=0) {
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

		NoofCols = dbDetails.getNoofCols();
		fieldlen = dbDetails.getFieldlen();
					
		System.out.println("file name :"+fileName);
		if (new File(fileName).exists()
				&& new File(fileName).length()!=0) {
			colList = new ArrayList<List<String>>();
			Map<String, String> map = new LinkedHashMap<>();
			//String inputfile = fileName;
			System.out.println("******fileNmae :"+fileName);
			File file = new File(fileName);
			//File output = new File(inputfile.replace(".csv", "_Output.csv"));
			//writing file on local
			String inputfile="";
			 inputfile = fileName.replace("\\", "|");
			 inputfile = fileName.replace("/", "|");
			 int index = inputfile.indexOf(".");
             inputfile = inputfile.substring(0, index);
             String sampleFile = inputfile + ".SAMPLE_FILE";

		//	 inputfile = inputfile.replaceAll(".csv", FileReaderConstant.SAMPLE_FILE);
			//File file = new File();
			String[] strArr=inputfile.split("\\|");
		//	String localPath=ConfigurationReader.getProperty("APP_DIR")+"/"+strArr[strArr.length-1];
			String localPath = strArr[strArr.length - 1];
			System.out.println("local path :**************:"+localPath);
			FileWriter writer = null;
			String[] header = null;
			try {
			if(!isFileWritten){
				writer=new FileWriter(new File(sampleFile));
			}
			BufferedReader br = new BufferedReader(new FileReader(file));
			System.out.println("Fixed Length file :  file:"+file);
			line = br.readLine();
			if (line != null) {
				header = line.split(",");
				for (int i = 0; i < header.length; i++) {
//					header[i] = header[i].replaceAll("[^a-zA-Z0-9 ]", "").trim();
//					header[i] = header[i].replaceAll("[ ]+", "_");
			}
	    	}
			int lineCount = 1;
			String line = br.readLine();
			if (!isFileWritten && line != null) {
				writer.write(line);
			}
				
				while (line != null && fieldlen != null ) {					
				String fielditem[] = fieldlen.split(",");  // splitting the field length based on ","(comma) separation
													
				int[] intarray = new int[fielditem.length];		
				
				int i, l, k;
				
				i = l = k = 0;
				
				for (String str : fielditem) {	      
				
					intarray[i] = Integer.parseInt(str);					
					
					String strout = line.substring(l, intarray[i] + l); //splitting the fixed length record based on fieldlen
					
					l = intarray[i]+l;	
                    
                    colList.add(new ArrayList());
                                        
                    colList.get(k).add(strout); // Adding the delimited string to output array list
                    
                     System.out.println(colList.get(k).toString().replace("[", "").replace("]", ""));
                    
                    k ++;  
                    
                    i ++;
                    
                }
				
				
				//logger.info("Total nUmber of record read :" + lineCount);
				
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
			if(!isFileWritten)
			  writer.close();
			logger.info("Total nUmber of record read :" + lineCount);
			}catch(Exception e){
				e.printStackTrace();
			//logger.error(e.toString());
			//String error=e.getMessage();
			//if(error.contains(":")){
			//	error= error.substring(0, error.indexOf(":"));
			//}
			throw new ZeasFileNotFoundException(ZeasErrorCode.FILE_NOT_FOUND,"error","");
		   }
			// put default column name and and respective datatypes
			int col = 0;
			for (int i = 0; i < colList.size(); i++) {
				++col;
				map.put(FileReaderConstant.DEFAULT_COLUMN + col,
						FileReaderUtility.columnDataType(colList.get(i)));
			}
            logger.info("getColumnAndDataType method started");
			return FileReaderUtility.getColumnNameAndDataType(header, map);
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
        ext.setNoofCols(3);
        ext.setFieldlen("3,5,4");
        ext.setFileName("C:\\TestData\\test1.txt");
        String fileName = "C:\\TestData\\test1.txt";
        
        try {
              dfr.getColumnAndDataType(fileName, ext);
        } catch (ZeasFileNotFoundException e) {  
              // TODO Auto-generated catch block
              e.printStackTrace();
        }
        
  }


}
