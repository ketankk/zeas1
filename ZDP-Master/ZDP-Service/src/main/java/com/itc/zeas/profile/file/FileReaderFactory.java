package com.itc.zeas.profile.file;

import com.itc.zeas.utility.filereader.FileReaderConstant;
import com.itc.zeas.ingestion.automatic.file.parsers.FixedFileReader;
import com.itc.zeas.ingestion.automatic.file.IFileDataTypeReader;
import com.itc.zeas.ingestion.automatic.file.json.JSONFileReader;
import com.itc.zeas.ingestion.automatic.file.xml.XmlFileReader;
import com.itc.zeas.ingestion.automatic.file.parsers.CSVFileReader;
import com.itc.zeas.ingestion.automatic.file.parsers.DelimitedFilesReader;
import com.itc.zeas.ingestion.automatic.file.parsers.ExcelFileReader;
import com.itc.zeas.profile.rdbms.RdbmsReader;

/*
 * This class creates file reader object.
 */
public class FileReaderFactory {

	public FileReaderFactory() {

	}

	// Returns the respective reader object on the basis of file extension
	public IFileDataTypeReader getFileReader(String fileType) {

		IFileDataTypeReader fileReaderObject = null;
		if (fileType != null) {

			if (fileType.contains(".")) {
				String fileExtension = fileType
						.substring(fileType.indexOf(".")).toLowerCase();
				if (fileExtension
						.equalsIgnoreCase(FileReaderConstant.RDBMS_TYPE)) {
					fileReaderObject = new RdbmsReader();
					return fileReaderObject;
				}
			}

			switch (fileType) {

			case FileReaderConstant.XLS_FILETYPE:
				fileReaderObject = new ExcelFileReader();
				//fileReaderObject = new ExcelFileParser();
				break;
			case FileReaderConstant.XLSX_FILETYPE:
				fileReaderObject = new ExcelFileReader();
				break;
			case FileReaderConstant.CSV_FILETYPE:
				fileReaderObject = new CSVFileReader();
				break;
			case FileReaderConstant.JSON_FILETYPE:
				fileReaderObject = new JSONFileReader();
				break;
			case FileReaderConstant.XML_FILETYPE:
				fileReaderObject = new XmlFileReader();
				break;
			case FileReaderConstant.FIXED_LENGTH_FILETYPE:
				fileReaderObject = new FixedFileReader();
				break;
			case FileReaderConstant.DELIMITED_FILETYPE:
				fileReaderObject = new DelimitedFilesReader();
				break;

			}

		}
		return fileReaderObject;
	}

}
