package com.itc.zeas.ingestion.automatic.file;

import java.util.List;
import java.util.Map;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.profile.model.ExtendedDetails;

/*
 * This interface helps to parse the file and take sample of data to detect datatype and header.
 */
public interface IFileDataTypeReader {
	
	 Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails) throws ZeasException;
	
	 List<List<String>> getColumnValues() throws ZeasException;
}
