
package com.taphius.validation.mr;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class ExcelMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> implements SheetContentsHandler {

	private static Logger LOG = LoggerFactory.getLogger(ExcelMapper.class);
	private StringBuilder builder;
	Mapper<LongWritable, Text, LongWritable, Text>.Context context;
	private long start;
	 /**
     * Excel Spreadsheet is supplied in string form to the mapper.
     * We are simply emitting them for viewing on HDFS.
     */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws InterruptedException, IOException {
		context.write(null,value);
		LOG.info("Map processing finished");
	}
	@Override
	public void run(
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		//calling super setUp method.
		setup(context);
		
		this.context=context;
		Configuration conf = context.getConfiguration();
		FileSplit split = (FileSplit) context.getInputSplit();
		 start=split.getStart();
		final Path file = split.getPath();

		FileSystem fs = file.getFileSystem(conf);
		String fileName=split.getPath().getName();
		boolean isHSSf=true;
		if(fileName.endsWith("xlsx")){
			isHSSf=false;
		}
		
		// read path from conf  
		//Path path = Paths.get("textPath");
		FSDataInputStream fileIn = fs.open(split.getPath());
		fileIn.seek(start);
		if(isHSSf){
			readXLSFile(fileIn,context);
			}else{
			    readXLSXFile(fileIn);    
			}
		
		//calling super cleanup method
		cleanup(context);
		
	}
	
	
	// Parsing 2013 excel file.
	private void readXLSXFile(InputStream is) {
		 OPCPackage pkg;
			try {
				pkg = OPCPackage.open(is);
				XSSFReader reader = new XSSFReader(pkg);

		        StylesTable styles = reader.getStylesTable();
		        ReadOnlySharedStringsTable sharedStrings = new ReadOnlySharedStringsTable(pkg);
		        ContentHandler handler = new XSSFSheetXMLHandler(styles, sharedStrings,this, true);

		        XMLReader parser = XMLReaderFactory.createXMLReader();
		        parser.setContentHandler(handler);

		        parser.parse(new InputSource(reader.getSheetsData().next()));

		        pkg.close();
		        is.close();
			} catch ( IOException | SAXException | OpenXML4JException e) {
				e.printStackTrace();
			}
		
	}
	
	
	// Parsing old version of excel file 2003 excel file.
	private void readXLSFile(InputStream is,Context context) {
		final DataFormatter formatter = new DataFormatter();
		try {
			HSSFWorkbook workbook = new HSSFWorkbook(is);

			// Taking first sheet from the workbook
			HSSFSheet sheet = workbook.getSheetAt(0);

			// Iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();
			
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
				map(new LongWritable(start), new Text(currentString.toString()), context);
				//context.write(new LongWritable(start), new Text(currentString.toString()));
				start++;
			}
			is.close();
		} catch (IOException | InterruptedException e) {
			LOG.error("IO Exception : File not found " + e);
		}
	}
	@Override
	public void cell(String cellReference, String formattedValue) {
		
		builder.append(formattedValue+"\t");
	}

	@Override
	public void endRow() {
		try {
			map(new LongWritable(start), new Text(builder.toString()),context);
			//context.write(new LongWritable(start), new Text(builder.toString()));
			start++;
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void headerFooter(String arg0, boolean arg1, String arg2) {

	}

	@Override
	public void startRow(int rowNum) {
		builder=new StringBuilder();
	}
}
