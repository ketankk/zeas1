package com.itc.zeas.filereader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.itc.taphius.utility.ConfigurationReader;

public class XmlToCsv extends DefaultHandler implements Runnable{
	
	private String fileName;
	private String tmpValue;
	private boolean isLast;
	private boolean isSmallFile;
	private int writeCount=0;
	private Map<String, List<String>> colNameAndValues;
	private String outSmallFile;
	private String outLargeFile;
	//private FileWriter foutLarge = null;
	private FileWriter foutsmall = null;
	private Logger logger=(Logger) Logger.getLogger(XmlToCsv.class);
	private String xmlEndTag="";
	private int tagCount;
		
	// this is a user defined Exception class used for logically break the reading at 1000th record 
	//of file by catching the exception
	
	@SuppressWarnings("serial")
	private class DoneParsingException extends SAXException {
		
	}
	
	@Override
	public void run() {
		writeToCsvBigFile();		
	}
	
	public XmlToCsv(String XmlFileName,boolean isSmall) {
		
		this.fileName = XmlFileName;
		outSmallFile = fileName.replace(".xml",FileReaderConstant.SAMPLE_FILE);
		outLargeFile = fileName.replace(".xml",".csv");
// making changes for unix system		
	 	outSmallFile = outSmallFile.replace("\\", "|");
		outSmallFile = outSmallFile.replace("/", "|");
		outLargeFile = outLargeFile.replace("\\", "|");
		outLargeFile = outLargeFile.replace("/", "|");		
		//File file = new File();
		String[] smallArr=outSmallFile.split("\\|");
		outSmallFile=ConfigurationReader.getProperty("APP_DIR")+"/"+smallArr[smallArr.length-1];
		String[] largeArr=outLargeFile.split("\\|");
		outLargeFile=ConfigurationReader.getProperty("APP_DIR")+"/"+largeArr[largeArr.length-1];
		System.out.println("local path small :**************:"+outSmallFile);
		System.out.println("local path large :**************:"+outLargeFile);		
//end
		this.isSmallFile=isSmall;
		colNameAndValues = new LinkedHashMap<>();
	}
	
	private void writeToCsvBigFile() {
		
		File bigFile = new File(outLargeFile);
		if (bigFile.exists()) {
			bigFile.delete();
		}		
		/*try {
			foutLarge = new FileWriter(bigFile);
		} catch (IOException e2) {
			e2.printStackTrace();
		}*/
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser = null;
		try {
			parser = factory.newSAXParser();
		} catch (ParserConfigurationException | SAXException e1) {
			e1.printStackTrace();
		}
		try {
			parser.parse(fileName, this);
		} catch ( DoneParsingException e) {		
			
			logger.info("----File Having more than 1000 records----Reading done for first 1000 records-----");
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			
			logger.error("Sax Exception failed to parce -------->"+e.toString());
			if (bigFile.exists()) {
				bigFile.delete();
			}	
			
			logger.info("Deleted the "+outLargeFile+ " file due to parse failuer");
			
		}
		if (isLast) {
			cleanData();
			try{
			writeToCsv();
		//	foutLarge.close();
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * This method will write the csv file after converting xml file to csv format.
	 * It will write full csv file and also sample file having 1000 records.
	 * uses multithreading for parallel writing of csv file and get the datatype from file
	 * uses map containing key as tagname and value as a list of element present in tag
	 * @throws IOException 
	 */
	private void writeToCsv() throws IOException {
		
		StringBuilder sb = new StringBuilder("");
		// find the highest length of columns
		int max = 0;
		Set<String> keys = colNameAndValues.keySet();
		for (String key : keys) {
			int temp = colNameAndValues.get(key).size();
			if (temp > max) {
				max = temp;
			}
		}
		
		int colSize=colNameAndValues.size();
		//prepare the string builder.
		for(int i=0;i<max;i++){
			StringBuilder tempStr= new StringBuilder("");
			int count=1;
			for (String key : keys) {
				String strVal="";
				try{
				strVal=colNameAndValues.get(key).get(i);
				}catch(Exception e){
					
				}
				tempStr.append(strVal);
				if(count<colSize){
					tempStr.append(",");
				}
				count++;
			}
			tempStr.append("\n");
			sb.append(tempStr);
		}
		if(writeCount<1){
			foutsmall= new FileWriter(outSmallFile);		
			foutsmall.write(sb.toString());
			foutsmall.close();
			logger.info("Sample data written to sample file as csv --------"+outSmallFile);
			writeCount++;
		}
		//foutLarge.write(sb.toString());
	}
	
	private void cleanData() {
		List<String> tempKey = new ArrayList<>();
		for (Entry<String, List<String>> entry : colNameAndValues.entrySet()) {
			List<String> values = entry.getValue();
			if (values.get(0).equals("") || values.size() == 0) {
				tempKey.add(entry.getKey());
			}
		}
		if(tempKey.size()>1){
			xmlEndTag=tempKey.get(tempKey.size()-2);
		}
		else xmlEndTag=tempKey.get(tempKey.size()-1);
		
		for (String key : tempKey) {
			colNameAndValues.remove(key);
		}
	}
	@Override
	public void startElement(String s, String s1, String elementName,
			Attributes attributes) throws SAXException {
		tagCount=0;
		int count = attributes.getLength();
		for (int i = 0; i < count; i++) {
			String name = attributes.getQName(i);
			String val = attributes.getValue(i).trim();
			if (colNameAndValues.containsKey(name)) {
				if (colNameAndValues.get(name).size() <= 1000) {
					if (colNameAndValues.get(name).get(0).isEmpty()) {
						if (!(val == null || val.isEmpty())) {
							colNameAndValues.get(name).add(val);
						}
					} else
						colNameAndValues.get(name).add(val);
				} else {
					if (isSmallFile) {
						throw new DoneParsingException();
					}
					isLast = false;
					cleanData();
					try {
						writeToCsv();
					} catch (IOException e) {
						e.printStackTrace();
					}
					colNameAndValues = new LinkedHashMap<>();
					colNameAndValues.put(name, new ArrayList<String>());
					if (val == null || val.isEmpty()) {
						colNameAndValues.get(name).add("");
					} else {
						colNameAndValues.get(name).add(val);
					}
				}
			} else {
				colNameAndValues.put(name, new ArrayList<String>());

				if (val == null || val.isEmpty()) {
					colNameAndValues.get(name).add("");
				} else {
					colNameAndValues.get(name).add(val);
				}
			}
		}
	}

	@Override
	public void endElement(String s, String s1, String element)
			throws SAXException {
		String name = element;
		String val = tmpValue.trim();
		isLast = true;
		if (colNameAndValues.containsKey(name)) {
			if (colNameAndValues.get(name).size() <= 1000) {
				if (colNameAndValues.get(name).get(0).isEmpty()) {
					if(tagCount==0){
						colNameAndValues.get(name).remove(0);
						colNameAndValues.get(name).add(0, "  ");
					}
					if (!(val == null || val.isEmpty())) {
						colNameAndValues.get(name).add(val);
					}
				} else
					colNameAndValues.get(name).add(val);
			} 
			else {
				if (isSmallFile) {
					throw new DoneParsingException();
				}
				isLast = false;
				cleanData();
				try {
					writeToCsv();
				} catch (IOException e) {
					e.printStackTrace();
				}
				colNameAndValues = new LinkedHashMap<>();
				colNameAndValues.put(name, new ArrayList<String>());
				if (val == null || val.isEmpty()) {
					colNameAndValues.get(name).add("");
				} else {
					colNameAndValues.get(name).add(val);
				}
			}
		} else {
			colNameAndValues.put(name, new ArrayList<String>());
			if (val == null || val.isEmpty()) {
				colNameAndValues.get(name).add("");
			} else {
				colNameAndValues.get(name).add(val);
			}
		}
	}
	@Override
	public void characters(char[] ac, int i, int j) throws SAXException {
		tmpValue = new String(ac, i, j).trim();
		tagCount++;
	}	
	
	public Map<String, List<String>> getColumnNameAndDataType(){
		return colNameAndValues;
	}
	public String getEndTag(){
		return xmlEndTag;
		
	}
}
