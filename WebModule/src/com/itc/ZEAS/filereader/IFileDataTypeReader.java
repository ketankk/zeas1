package com.itc.zeas.filereader;

import java.util.List;
import java.util.Map;

import com.itc.zeas.exception.ZeasException;

/*
 * This interface helps to parse the file and take sample of data to detect datatype and header.
 */
public interface IFileDataTypeReader {
	
	public Map<String, String> getColumnAndDataType(String fileName, ExtendedDetails dbDetails) throws ZeasException;  
	
	public List<List<String>> getColumnValues() throws ZeasException;
}
