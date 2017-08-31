package com.itc.zeas.utility.filereader;

/**
 * 
 * @author 20597
 * DB2 added
 *
 */
/*
 * constant list for file reader.
 */
public interface FileReaderConstant {

	String XLS_TYPE=".xls";
	String XLSX_TYPE=".xlsx";
	String CSV_TYPE=".csv";
	String DEFAULT_COLUMN="column";
	String DATATYPE_INT="int";
	String DATATYPE_LONG="long";
	String DATATYPE_DOUBLE="double";
	String DATATYPE_STRING="string";
	String DATATYPE_DATE="date";
	String DATATYPE_TIMESTAMP="timestamp";
	String DATATYPE_BOOLEAN="boolean";
	
	Integer INT_RANGE=5;// means length of int value greater than 5 considered long value
	String JSON_TYPE=".json";
	String XML_TYPE=".xml";
	String SAMPLE_FILE="_sample.csv";
	String RDBMS_TYPE=".rdbmstype";
	
	
	String XLS_FILETYPE="XLS";
	String XLSX_FILETYPE="XLSX";
	String XML_FILETYPE="XML";
	String CSV_FILETYPE="CSV";
	String JSON_FILETYPE="JSON";
	String MYSQL_FILETYPE="MySQL";
	String FIXED_LENGTH_FILETYPE="Fixed Width";
	String DELIMITED_FILETYPE="Delimited";

	
	// Constants used for RDBMS implementation 
	String MYSQL_TYPE="MySQL";
	String ORACLE_TYPE="Oracle";
	String DB2_TYPE="DB2";

	String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	
	
	String DATATYPE_NOT_SUPPORTED = "notSupported";
}