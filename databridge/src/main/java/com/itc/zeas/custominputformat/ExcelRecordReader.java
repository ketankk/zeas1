/*
 * Copyright 2014 Sreejith Pillai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.itc.zeas.custominputformat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;

/**
 * Reads excel spread sheet , where keys are offset in file and value is the row
 * containing all column as a string.
 */
public class ExcelRecordReader extends RecordReader<LongWritable, Text> {

	private LongWritable key;
	private Text value;
	private InputStream is;
	HSSFWorkbook workbook ;
	Iterator<Row> rowIterator;
	long start;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();

		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);
		start=split.getStart();
		is = fileIn;
		is.close();
		fileIn.close();
		/*workbook= new HSSFWorkbook(is);

		// Taking first sheet from the workbook
		 HSSFSheet sheet = workbook.getSheetAt(0);
		  rowIterator= sheet.iterator();*/
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		return true;
		/*final DataFormatter formatter = new DataFormatter();
		
		
		String thisStr = "";
		while (rowIterator.hasNext()) {
			StringBuilder currentString = new StringBuilder();
			Row row = rowIterator.next();

			// For each row, iterate through each columns
			Iterator<Cell> cellIterator = row.cellIterator();

			while (cellIterator.hasNext()) {
				
				Cell cell = cellIterator.next();

				switch (cell.getCellType()) {
				case Cell.CELL_TYPE_BOOLEAN:
					currentString.append(cell.getBooleanCellValue() + "\t");
					break;

				case Cell.CELL_TYPE_NUMERIC:
					if (DateUtil.isCellDateFormatted(cell)) {
						

						DataFormatter df = new DataFormatter();
						thisStr=df.formatCellValue(cell);
					} else{
						thisStr = formatter
								.formatRawCellContents(
										cell.getNumericCellValue(),
										cell.getCellStyle()
												.getDataFormat(),
										cell.getCellStyle()
												.getDataFormatString());
					
					}
					if (thisStr.contains(",")) {
						thisStr = thisStr.replaceAll(",", "");
					}
					currentString.append(thisStr + "\t");
					break;

				case Cell.CELL_TYPE_STRING:
					currentString.append(cell.getStringCellValue() + "\t");
					break;
				case Cell.CELL_TYPE_FORMULA:

					if (workbook.getCreationHelper()
							.createFormulaEvaluator()
							.evaluate(cell).getCellType() == Cell.CELL_TYPE_NUMERIC) {
						thisStr = formatter
								.formatRawCellContents(
										cell.getNumericCellValue(),
										cell.getCellStyle()
												.getDataFormat(),
												cell.getCellStyle()
												.getDataFormatString());
						
						currentString.append(thisStr + "\t");
					}

					else if (workbook.getCreationHelper()
							.createFormulaEvaluator()
							.evaluate(cell).getCellType() == Cell.CELL_TYPE_STRING) {
						thisStr = cell.getStringCellValue();
						if (thisStr.contains(",")) {
							thisStr = thisStr.replaceAll(",", "");
						}
						currentString.append(thisStr + "\t");
						break;
					}

				}
			}
			if(key==null || value==null){
				key=new LongWritable();
				value=new Text();
			}
			key.set(start);
			value.set(currentString.toString());
			start++;
			return true;
		}
			return false;*/
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return 0;
		

	}

	@Override
	public void close() throws IOException {
		if (is != null) {
			is.close();
		}

	}

}
