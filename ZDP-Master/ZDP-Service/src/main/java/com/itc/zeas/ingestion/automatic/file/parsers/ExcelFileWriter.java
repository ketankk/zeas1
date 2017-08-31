package com.itc.zeas.ingestion.automatic.file.parsers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.file.xlx.XlsxToCsv;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.xml.sax.SAXException;

import com.itc.zeas.utility.utility.ConfigurationReader;

public class ExcelFileWriter implements Runnable {
	String FileName;
	Boolean limitCount;
	String CSVFile;
	Logger logger = Logger.getLogger("ExcelFileWriter");

	public ExcelFileWriter(String fileName, Boolean limit) {
		this.FileName = fileName;
		this.limitCount = limit;
	}

	@Override
	public void run() {
		try {
			csvWriter();
		} catch (IOException | OpenXML4JException
				| ParserConfigurationException | SAXException e) {

			e.printStackTrace();
		}
	}

	/**
	 * This method will write the csv file after converting Excel sheet to csv
	 * format. It will write two different csv file one for sample data i.e.
	 * 1000 lines another complite file. uses multithreading for parallel
	 * writing of csv file.
	 * 
	 * @throws IOException
	 * @throws OpenXML4JException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 */
	private void csvWriter() throws IOException, OpenXML4JException,
			ParserConfigurationException, SAXException {
		String outSmallFile = null;
		String outSmallFile_1 = null;
		String outLargeFile = null;

		if (FileName.endsWith("xlsx")) {

			PrintStream pr;
			File xlsxFile = new File(FileName);
			// File destinationFile = new File("output.csv");
			int minColumns = -1;
			minColumns = 2;
			// The package open is instantaneous, as it should be.
			OPCPackage p = OPCPackage.open(xlsxFile.getPath(),
					PackageAccess.READ);
			XSSFReader xssfReader = new XSSFReader(p);
			XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader
					.getSheetsData();
			int count = 1;
			while (iter.hasNext()) {

				outSmallFile = FileName.replace(".xlsx", "_" + count
						+ FileReaderConstant.SAMPLE_FILE);
				outLargeFile = FileName.replace(".xlsx", "_" + count + ".csv");

				// test

				outSmallFile = outSmallFile.replace("\\", "|");
				outSmallFile = outSmallFile.replace("/", "|");
				// outLargeFile = outLargeFile.replace("\\", "|");
				// outLargeFile = outLargeFile.replace("/", "|");

				// File file = new File();
				String[] smallArr = outSmallFile.split("\\|");
				outSmallFile = ConfigurationReader.getProperty("APP_DIR") + "/"
						+ smallArr[smallArr.length - 1];
				// String[] largeArr=outLargeFile.split("\\|");
				// outLargeFile=ConfigurationReader.getProperty("APP_DIR")+"/"+largeArr[largeArr.length-1];
				System.out.println("local path small :**************:"
						+ outSmallFile);
				System.out.println("local path large :**************:"
						+ outLargeFile);

				// end
				if (count == 1) {
					outSmallFile_1 = outSmallFile;
				}
				// String sheetName = iter.getSheetName()
				InputStream stream = iter.next();
				if (limitCount) {
					pr = new PrintStream(outSmallFile);
					logger.info("Preparing to Write sample CSV file from XLSX file as Output File Name "
							+ outSmallFile);
					XlsxToCsv xlsx2csv = new XlsxToCsv(p, pr, stream,
							minColumns, true);
					xlsx2csv.process();
					logger.info(" Output sample File Written Successfuly as  "
							+ outSmallFile);
				} else {
					pr = new PrintStream(outLargeFile);
					logger.info("Preparing to Write the CSV file from XLSX file as Output File Name "
							+ outLargeFile);
					XlsxToCsv xlsx2csv = new XlsxToCsv(p, pr, stream,
							minColumns, false);
					xlsx2csv.process();
					logger.info(" Output File Written Successfuly as  "
							+ outLargeFile);
					if (xlsxFile.exists()) {
						xlsxFile.delete();
					}
				}
				++count;
			}
		} else if (FileName.endsWith("xls")) {

			PrintWriter pr;
			StringBuilder ColumValueList = new StringBuilder();

			String thisStr = null;
			Sheet worksheet = null;
			final DataFormatter formatter = new DataFormatter();
			NPOIFSFileSystem npoifsFileSystem =null;
			try{
			 npoifsFileSystem = new NPOIFSFileSystem(new File(
					FileName));
			HSSFWorkbook workbookh = new HSSFWorkbook(
					npoifsFileSystem.getRoot(), true);
			int numSheet = workbookh.getNumberOfSheets();
			for (int m = 0; m < numSheet; m++) {
				outSmallFile = FileName.replace(".xls", "_" + (m + 1)
						+ FileReaderConstant.SAMPLE_FILE);
				outLargeFile = FileName.replace(".xls", "_" + (m + 1) + ".csv");

				// test

				outSmallFile = outSmallFile.replace("\\", "|");
				outSmallFile = outSmallFile.replace("/", "|");
				// outLargeFile = outLargeFile.replace("\\", "|");
				// outLargeFile = outLargeFile.replace("/", "|");
				// File file = new File();
				String[] smallArr = outSmallFile.split("\\|");
				outSmallFile = ConfigurationReader.getProperty("APP_DIR") + "/"
						+ smallArr[smallArr.length - 1];
				// String[] largeArr=outLargeFile.split("\\|");
				// outLargeFile=ConfigurationReader.getProperty("APP_DIR")+"/"+largeArr[largeArr.length-1];
				System.out.println("local path small xls :**************:"
						+ outSmallFile);
				System.out.println("local path large xls:**************:"
						+ outLargeFile);

				// end
				if ((m + 1) == 1) {
					outSmallFile_1 = outSmallFile;
				}
				worksheet = workbookh.getSheetAt(m);

				int numColumn = 0;
				int numRow = 0;
				if (limitCount) {
					Iterator<Row> numrow = worksheet.iterator();
					while (numrow.hasNext()) {
						Row rs = numrow.next();
						if (numRow > 999) {
							break;
						}
						numRow++;
					}
					logger.info("No of Rows for sample file-->>>>" + numRow);
					pr = new PrintWriter(outSmallFile);
				} else {
					pr = new PrintWriter(outLargeFile);
					Iterator<Row> numrow = worksheet.iterator();
					while (numrow.hasNext()) {
						Row rs = numrow.next();
						numRow++;
					}
					logger.info("Total No of Rows for complite file-->>>>"
							+ numRow);
				}

				if (numRow == 0) {
					numColumn = 0;
					ColumValueList.append("");
				}

				for (int l = 0; l < numRow; l++) {

					Row rown = worksheet.getRow(l);
					if (rown != null) {

						Iterator<Cell> cells = rown.cellIterator();
						while (cells.hasNext()) {
							Cell column = cells.next();

							if (column != null) {
								switch (column.getCellType()) {

								case Cell.CELL_TYPE_NUMERIC:
									if (DateUtil.isCellDateFormatted(column)) {
										

										DataFormatter df = new DataFormatter();
										thisStr=df.formatCellValue(column);
									} else{
										thisStr = formatter
												.formatRawCellContents(
														column.getNumericCellValue(),
														column.getCellStyle()
																.getDataFormat(),
														column.getCellStyle()
																.getDataFormatString());
									
									}
									if (thisStr.contains(",")) {
										thisStr = thisStr.replaceAll(",", "");
									}
									ColumValueList.append(thisStr);

									break;
								case Cell.CELL_TYPE_STRING:
									thisStr = column.getStringCellValue();
									if (thisStr.contains(",")) {
										thisStr = thisStr.replaceAll(",", "");
									}
									ColumValueList.append(thisStr);

									break;
								case Cell.CELL_TYPE_BOOLEAN:

									ColumValueList.append(column
											.getBooleanCellValue());

									break;

								case Cell.CELL_TYPE_BLANK:

									ColumValueList.append("Blank");
									break;

								case Cell.CELL_TYPE_FORMULA:

									if (workbookh.getCreationHelper()
											.createFormulaEvaluator()
											.evaluate(column).getCellType() == Cell.CELL_TYPE_NUMERIC) {
										thisStr = formatter
												.formatRawCellContents(
														column.getNumericCellValue(),
														column.getCellStyle()
																.getDataFormat(),
														column.getCellStyle()
																.getDataFormatString());
										if (thisStr.contains(",")) {
											thisStr = thisStr.replaceAll(",", "");
										}
										ColumValueList.append(thisStr);
									}

									else if (workbookh.getCreationHelper()
											.createFormulaEvaluator()
											.evaluate(column).getCellType() == Cell.CELL_TYPE_STRING) {
										thisStr = column.getStringCellValue();
										if (thisStr.contains(",")) {
											thisStr = thisStr.replaceAll(",", "");
										}
										ColumValueList.append(thisStr);
										break;
									}
								default:
									ColumValueList.append("  ");
								}
							}
							ColumValueList.append(",");
						}
					}
					ColumValueList.delete(ColumValueList.length()-1, ColumValueList.length());
					ColumValueList.append(System.lineSeparator());
				}
				pr.print(ColumValueList);
				pr.close();
			} 
		} 
			//added  try and finally to close the npoifsFileSystem. 
		finally{
			//close npoifsFileSystem after checking if it is not null
			if(npoifsFileSystem!=null){
			npoifsFileSystem.close();
			}
		}
			
		}
		CSVFile = outSmallFile_1;
		if (limitCount) {
			logger.info("Excel to sample CSV file converted successfuly Output stored as   ------>"
					+ outSmallFile);
		} else {
			logger.info("Excel to complite CSV file converted successfuly Output stored as ------>"
					+ outLargeFile);
		}
	}
}
