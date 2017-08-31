/*
 * File Name: SampleDataView.java Description:
 * @author: 18947 Created: 26-Mar-2015 ----------------------------- -----------------------------
 */

package com.itc.zeas.filereader;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.itc.zeas.exception.ZeasErrorCode;
import com.itc.zeas.exception.ZeasException;
import com.itc.zeas.exception.ZeasFileNotFoundException;
import com.itc.zeas.pii.PIIDetection;

public class SampleDataView {

	private String fileName = "";
	private ExtendedDetails dataBaseDetails;
	Logger logger = Logger.getLogger("SampleDataView");

	public SampleDataView(String fileName, ExtendedDetails dbDeatails) {

		this.fileName = fileName;
		this.dataBaseDetails = dbDeatails;
	}

	/**
	 * Returns the sample data with header and datatype.
	 * 
	 * @author: 18947 Date: 26-Mar-2015
	 * @throws Exception
	 */
	public List<List<String>> getSampleData() throws ZeasException {

		// check file exist or not

		if (!fileName.equals(FileReaderConstant.RDBMS_TYPE)) {

			File tempFileName = new File(fileName);
			if (!tempFileName.exists()) {
				throw new ZeasFileNotFoundException(
						ZeasErrorCode.FILE_NOT_FOUND,
						"file not found : filename=>", fileName);
			}
		}

		FileReaderFactory readerFactory = new FileReaderFactory();
		System.out.println("sample data view :" + fileName);
		List<List<String>> sampleDataLis = new ArrayList<>();
		List<String> headerList = new ArrayList<>();
		List<String> dataTypeList = new ArrayList<>();
		PIIDetection pii = new PIIDetection();
		Map<String, String> outputMap = new LinkedHashMap<String, String>();
		List<String> piiList = new ArrayList<>();

		int index = -1;
		String fileExtn = "";
		if (fileName != null) {
			index = fileName.indexOf(".");
		}
		if (index >= 0) {
			fileExtn = fileName.substring(index);
		}
		String fileType = dataBaseDetails.getFileType();
		if (fileType.equalsIgnoreCase(FileReaderConstant.CSV_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.JSON_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.XML_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.XLS_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.MYSQL_FILETYPE)
				|| fileType
						.equalsIgnoreCase(FileReaderConstant.DELIMITED_FILETYPE)
				|| fileType
						.equalsIgnoreCase(FileReaderConstant.FIXED_LENGTH_FILETYPE)
				|| fileExtn.equalsIgnoreCase(FileReaderConstant.RDBMS_TYPE)) {
			logger.info("getSampleData method start execution: " + fileName
					+ " File Type:" + (fileType));
			IFileDataTypeReader fileReader;
			if (fileExtn.equalsIgnoreCase(FileReaderConstant.RDBMS_TYPE)) {
				fileReader = readerFactory.getFileReader(fileName);
			} else {
				fileReader = readerFactory.getFileReader(fileType);
			}
			Map<String, String> colNameAndDataType = fileReader
					.getColumnAndDataType(fileName, dataBaseDetails);

			List<List<String>> colValues = fileReader.getColumnValues();
			for (Entry<String, String> entry : colNameAndDataType.entrySet()) {
				// replacing space with (_) in header values
				String header = entry.getKey();
				headerList.add(header.trim().replaceAll("\\s+", "_"));
				dataTypeList.add(entry.getValue());
			}
			if (colNameAndDataType.size() > 0 & colValues.size() > 0) {
				outputMap = pii.findPII(colNameAndDataType, colValues);
				for (Entry<String, String> entry : outputMap.entrySet()) {
					piiList.add(entry.getValue());
				}
			} else {
				logger.warn("Failed to call findPII api. Column Header Map or Column Data List is empty. ");
			}
			sampleDataLis.add(0, headerList);
			sampleDataLis.add(1, dataTypeList);
			sampleDataLis.add(2, piiList);
			if (fileName.endsWith(".xml")) {
				List<String> endTag = new ArrayList<>();
				endTag.add(XmlFileReader.getXmlEndTag());
				sampleDataLis.add(3, endTag);
			}
			int recordSize = 0;
			if (colValues.size() != 0)
				recordSize = colValues.get(0).size();

			for (int i = 0; i < recordSize; i++) {

				List<String> temp = new ArrayList<>();
				int noOfColumn = headerList.size();
				for (int j = 0; j < noOfColumn; j++) {
					try {
						List<String> ll = colValues.get(j);
						temp.add(ll.get(i));
					} catch (Exception e) {
						temp.add("");
					}
				}
				sampleDataLis.add(temp);
			}
			logger.info("successfully read sample data :, colsize:"
					+ sampleDataLis.size());
		} else {
			logger.error("file extension is not valid , file name:" + fileName);
		}
		return sampleDataLis;
	}
}
