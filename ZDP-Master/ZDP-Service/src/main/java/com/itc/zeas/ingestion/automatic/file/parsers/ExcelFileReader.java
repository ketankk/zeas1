package com.itc.zeas.ingestion.automatic.file.parsers;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasException;

public class ExcelFileReader implements IFileDataTypeReader {

	Logger logger = Logger.getLogger("ExcelFileReader.class");
	CSVFileReader csvReader =null;
	private Map<String, String> colName;
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
			if(dbDetails.getmFlag().equalsIgnoreCase("true")){
				colName= new HashMap<>();
			}else{
				colName= csvReader.getColumnAndDataType(fname, true,dbDetails);
			}
			return colName;
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