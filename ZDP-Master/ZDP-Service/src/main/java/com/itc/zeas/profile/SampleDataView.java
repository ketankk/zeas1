/*
 * File Name: SampleDataView.java Description:
 * @author: 18947 Created: 26-Mar-2015 ----------------------------- -----------------------------
 */

package com.itc.zeas.profile;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.profile.file.FileReaderFactory;
import com.itc.zeas.utility.filereader.FileReaderUtility;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import com.itc.zeas.ingestion.automatic.file.xml.XmlFileReader;
import com.itc.zeas.profile.model.ExtendedDetails;
import org.apache.log4j.Logger;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasFileNotFoundException;
import com.itc.zeas.utility.pii.PIIDetection;
import com.itc.zeas.exceptions.ZeasException;

public class SampleDataView {

	private String fileName = "";
	private ExtendedDetails dataBaseDetails;
	private List<String> headerList;
	private List<String> dataTypeList;
	Logger logger = Logger.getLogger("SampleDataView");
	public static final String DOUBLE_QUOTE_CONSTANT = "\"";
	public SampleDataView(String fileName, ExtendedDetails dbDeatails,List<String> headList,List<String> typeList) {

		this.fileName = fileName;
		this.dataBaseDetails = dbDeatails;
		if(dbDeatails.getmFlag().equalsIgnoreCase("true") && headList!=null && typeList!=null){
			this.headerList=headList;
			this.dataTypeList = typeList;
		}else{
			this.headerList=new ArrayList<>();
			this.dataTypeList = new ArrayList<>();
		}
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
			
			System.out.println("file path :" + tempFileName.getPath());
			if (!tempFileName.exists()) {
				throw new ZeasFileNotFoundException(
						ZeasErrorCode.FILE_NOT_FOUND,
						"File not found : filename=>", fileName);
			}
		}

		FileReaderFactory readerFactory = new FileReaderFactory();
		//System.out.println("sample data view :" + fileName);
		List<List<String>> sampleDataLis = new ArrayList<>();
		/*List<String> headerList = new ArrayList<>();
		List<String> dataTypeList = new ArrayList<>();*/
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
		if ((fileType !=null && (fileType.equalsIgnoreCase(FileReaderConstant.CSV_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.JSON_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.XML_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.XLS_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.XLSX_FILETYPE)
				|| fileType.equalsIgnoreCase(FileReaderConstant.MYSQL_FILETYPE)
				|| fileType
						.equalsIgnoreCase(FileReaderConstant.DELIMITED_FILETYPE)
				|| fileType
						.equalsIgnoreCase(FileReaderConstant.FIXED_LENGTH_FILETYPE)))
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
			if(this.headerList.size()==0 || this.dataTypeList.size()==0){
			for (Entry<String, String> entry : colNameAndDataType.entrySet()) {
				// replacing space with (_) in header values
				String header = entry.getKey();
				this.headerList.add(header.trim().replaceAll("\\s+", "_"));
				this.dataTypeList.add(entry.getValue());
			}
			}
			if (colNameAndDataType.size() > 0 & colValues.size() > 0) {
				outputMap = pii.findPII(colNameAndDataType, colValues);
				for (Entry<String, String> entry : outputMap.entrySet()) {
					piiList.add(entry.getValue());
				}
			} else {
				logger.warn("Failed to call findPII api. Column Header Map or Column Data List is empty. ");
			}
			for(String headers:this.headerList){
				headers.replaceAll("[^0-9\\p{Alpha}]+","_");
			}
			sampleDataLis.add(0, this.headerList);
			sampleDataLis.add(1, this.dataTypeList);
			sampleDataLis.add(2, piiList);
			if (fileName.endsWith(".xml")) {
				List<String> endTag = new ArrayList<>();
				endTag.add(XmlFileReader.getXmlEndTag());
				sampleDataLis.add(3, endTag);
			}
			int recordSize = 0;
			if (colValues.size() != 0)
				recordSize = colValues.get(0).size();
			recordSize= FileReaderUtility.calculateNoOfRecordSize(colValues);
			/*Fix for bug id:https://github.com/itcbdlob/zdp/issues/22 */
			if (fileName.endsWith(".xml")) {
				int currentSize = 0;
				for (List<String> list : colValues) {
					currentSize = list.size();
					/*
					 *  if record size is less than actual size than update the
					 * value of record size.
					 */
					if (currentSize > recordSize) {
						recordSize = currentSize;
					}
				}
			}
			for (int i = 0; i < recordSize; i++) {

				List<String> temp = new ArrayList<>();
				int noOfColumn = this.headerList.size();
				for (int j = 0; j < noOfColumn; j++) {
					try {
						List<String> ll = colValues.get(j);
						//converting the double value which is having  value like (1.3434e+100, -323.323e-23) to double 123.434,323.777 etc
						//it is only for preview data not change  for file contents
						if(sampleDataLis.get(1).get(j).equalsIgnoreCase(FileReaderConstant.DATATYPE_DOUBLE)) {
							try {
								Double dd = Double.parseDouble(ll.get(i));
								temp.add(dd.toString());
							} catch (NumberFormatException e ) {
								temp.add("");
							}
						}
						else {
						temp.add(ll.get(i));
						}
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
