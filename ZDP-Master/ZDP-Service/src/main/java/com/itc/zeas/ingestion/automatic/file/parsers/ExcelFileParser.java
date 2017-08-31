package com.itc.zeas.ingestion.automatic.file.parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.profile.model.ExtendedDetails;
import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.itc.zeas.exceptions.ZeasException;

/**
 * This class will parse xls and xlsx.
 * this parsing is dedicated to dataset creation (dataset uses list of header with datatype and list of columns values).
 * @author Arvind(17038)
 *
 */

/**
 * @ToDo : check Header and check for negative values
 * @author 17038
 *
 */
public class ExcelFileParser implements IFileDataTypeReader {

	final DataFormatter formatter = new DataFormatter();

	Logger logger = Logger.getLogger("ExcelFileParser");

	@SuppressWarnings("rawtypes")
	List columnValue = new ArrayList<>();

	Map<String, String> headerWithDataType = null;

	public void parseXSLFile(String fileName, ExtendedDetails dbDetails) {
		try {
			FileInputStream file = new FileInputStream(new File(fileName));
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			XSSFSheet sheet = workbook.getSheetAt(0);

			int colLength = sheet.getRow(0).getLastCellNum();
			int rowLength = sheet.getLastRowNum();

			setColumnData(sheet, colLength, rowLength, dbDetails);
			setColumnAndDataType(sheet, colLength, rowLength, workbook, dbDetails);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * this method collected header values and dataType and stores in map. [
	 * Date_(mm/dd/yyyy) = date Shift = String
	 * Off_gas_consumption_(Nm3/Shift)=long Total_Off_gas_flare_(Nm3/Shift)=long
	 * CV_of_Tail_Gas_(kCal/Nm3)=string FO_Consumption = string ]
	 * 
	 * @param sheet
	 * @param colLength
	 * @param rowLength
	 * @param workbook
	 * @param dbDetails
	 * @return
	 */
	private Map<String, String> setColumnAndDataType(XSSFSheet sheet, int colLength, int rowLength,
			XSSFWorkbook workbook, ExtendedDetails dbDetails) {

		DataFormatter df = new DataFormatter();
		headerWithDataType = new LinkedHashMap<String, String>();

		// Map<String, String> columnWithType = new LinkedHashMap<String,
		// String>();
		Map<String, String> tempMap = new LinkedHashMap<String, String>();

		for (int c = 0; c < colLength; c++) {
			Row row = sheet.getRow(1);
			Cell cell = row.getCell(c);
			if (cell != null) {
				Row row1 = sheet.getRow(0);
				Cell cell1 = row1.getCell(c);

				switch (cell.getCellType()) {

				case Cell.CELL_TYPE_NUMERIC:
					if (DateUtil.isCellDateFormatted(cell)) {

						headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_DATE);
					} else {
						String cellValue = formatter.formatRawCellContents(cell.getNumericCellValue(),
								cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString());
						if (cellValue.contains("."))
							headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_DOUBLE);
						else
							headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_LONG);

					}
					break;

				case Cell.CELL_TYPE_STRING:
					headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_STRING);
					break;

				case Cell.CELL_TYPE_BOOLEAN:
					headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_BOOLEAN);
					break;

				case Cell.CELL_TYPE_FORMULA:

					if (workbook.getCreationHelper().createFormulaEvaluator().evaluate(cell)
							.getCellType() == Cell.CELL_TYPE_NUMERIC) {

						headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_LONG);
					}

					else if (workbook.getCreationHelper().createFormulaEvaluator().evaluate(cell)
							.getCellType() == Cell.CELL_TYPE_STRING) {
						headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_STRING);
						break;
					}
				case Cell.CELL_TYPE_BLANK:
					headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_STRING);
					break;

				default:

					if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
						switch (cell.getCachedFormulaResultType()) {
						case Cell.CELL_TYPE_NUMERIC:

							String s = formatter.formatRawCellContents(cell.getNumericCellValue(),
									cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString());
							headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_LONG);
							break;
						case Cell.CELL_TYPE_STRING:
							headerWithDataType.put(df.formatCellValue(cell1), FileReaderConstant.DATATYPE_STRING);
							break;
						}
					}

				}
			}
		}

		// headerWithDataType = columnWithType;
		// columnWithType =null;

		if (dbDetails.gethFlag() != null && dbDetails.gethFlag().equalsIgnoreCase("false")) {

			int index = 1;
			for (Map.Entry<String, String> entry : headerWithDataType.entrySet()) {
				tempMap.put(FileReaderConstant.DEFAULT_COLUMN + index, entry.getValue());

				++index;
			}
			;
			index = 0;
			headerWithDataType = tempMap;
			tempMap = null;
		}

		return headerWithDataType;

	}

	/**
	 * It return list of column's value 6/30/2015| A |355000 6/30/2015| B
	 * |359000 6/30/2015| C |350000
	 * 
	 * @param sheet
	 * @param colLength
	 * @param rowLength
	 * @param dbDetails
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List setColumnData(XSSFSheet sheet, int colLength, int rowLength, ExtendedDetails dbDetails) {

		for (int c = 0; c < colLength; c++) {
			List localList = new ArrayList<>();
			int r = 0;
			if (dbDetails.gethFlag().equalsIgnoreCase("true")) {
				r = 1;
			}
			for (; r < rowLength + 1; r++) {
				Row row = sheet.getRow(r);
				;
				Cell cell = row.getCell(c);
				if (cell != null) {
					String cellValue = null;
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_NUMERIC:

						cellValue = formatter.formatRawCellContents(cell.getNumericCellValue(),
								cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString());
						break;
					case Cell.CELL_TYPE_STRING:
						cellValue = cell.getStringCellValue();
						break;

					case Cell.CELL_TYPE_BLANK:
						cellValue = "";
						break;

					default:
						if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
							switch (cell.getCachedFormulaResultType()) {
							case Cell.CELL_TYPE_NUMERIC:

								cellValue = formatter.formatRawCellContents(cell.getNumericCellValue(),
										cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString());
								break;

							}
						}

					}
					localList.add(cellValue);

				}
			}
			columnValue.add(localList);

		}
		return columnValue;
	}

	public static void main(String[] args) throws Exception {
		ExcelFileParser t = new ExcelFileParser();
		t.excelData("D:\\metadata.xlsx");

	}

	@Override
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails) {

		System.out.println("dbDetails.gethFlag() == :" + dbDetails.gethFlag());
		parseXSLFile(fileName, dbDetails);

		return headerWithDataType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<List<String>> getColumnValues() throws ZeasException {
		return columnValue;
	}

	// parsing an .xlsx file
	public List<List<Object>> excelData(String path) throws IOException,Exception {
		FileInputStream file = new FileInputStream(new File(path));
		XSSFWorkbook workbook = new XSSFWorkbook(file);
		// Get first sheet from the workbook
		XSSFSheet sheet = workbook.getSheetAt(0);
		// Iterate through each rows from first sheet
		Iterator<Row> rowIterator = sheet.iterator();
		int maxNumOfCells = sheet.getRow(0).getLastCellNum(); // The the maximum number of columns
		List<List<Object>> FullData = new ArrayList<List<Object>>();
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			// For each row, iterate through each columns
			Iterator<Cell> cellIterator = row.cellIterator();
			List data = new ArrayList();
			for (int i = 0; i < maxNumOfCells; i++) {

				Cell cell = row.getCell(i);
				if (cell != null) {
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_BOOLEAN:
						data.add(cell.getBooleanCellValue());
						break;
					case Cell.CELL_TYPE_STRING:
						data.add(cell.getStringCellValue());
						break;
					case Cell.CELL_TYPE_NUMERIC:
						data.add(formatter.formatRawCellContents(cell.getNumericCellValue(),
								cell.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormatString()));
						break;
					case Cell.CELL_TYPE_BLANK:
						data.add(cell.toString());
						break;
					}
				} else {
					data.add(null);
				}

			}
			FullData.add(data);
		}
		System.out.println("FullData===" + FullData);
		file.close();
		return FullData;
	}

}
