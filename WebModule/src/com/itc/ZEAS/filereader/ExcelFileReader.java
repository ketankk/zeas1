package com.itc.zeas.filereader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.itc.zeas.exception.ZeasException;

public class ExcelFileReader implements IFileDataTypeReader {

	Logger logger = Logger.getLogger("ExcelFileReader.class");
	CSVFileReader csvReader =null;
	@Override
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails) throws ZeasException
			{
		
		csvReader = new CSVFileReader();
		if (new File(fileName).length() != 0 && new File(fileName).exists()) {
			ExcelFileWriter efw = new ExcelFileWriter(fileName, true);
			ExcelFileWriter efwf = new ExcelFileWriter(fileName, false);
			Thread t1 = new Thread(efw);
			Thread t2 = new Thread(efwf);
			t1.start();
			t2.start();
			String fname = "";
			while (true) {
				if (!t1.isAlive()) {
					fname = efw.CSVFile;
					break;
				}
			}
			System.out.println("filename :******* "+fname);
			return csvReader.getColumnAndDataType(fname, true);
		} else {
			logger.debug("File not Found" + fileName);
			return new HashMap<>();
		}
	}

	@Override
	public List<List<String>> getColumnValues() {
		return csvReader.getColumnValues();
	}


}