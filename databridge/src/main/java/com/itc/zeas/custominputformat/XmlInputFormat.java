package com.itc.zeas.custominputformat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlInputFormat extends TextInputFormat{
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext tac) {
		
		return	 new XmlRecordReader();
		
	}
	public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		private static final  String END_TAG_KEY ="xmlTag.end";
		public static final Log log = LogFactory.getLog(XmlRecordReader.class);
		private int startElement=0;

		/*public XmlRecordReader(String currentValue) {
			this.START_TAG_KEY=currentValue;
			this.END_TAG_KEY=START_TAG_KEY.replace("<", "</");
			
		}*/
		
		public XmlRecordReader() {
		}

		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit) is;
			
	         endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();

			FileSystem fs = file.getFileSystem(tac.getConfiguration());
			fsin = fs.open(fileSplit.getPath());
			
			fsin.seek(start);

		}

		@Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	         if (fsin.getPos() < end) {
	      try {
	        if (readUntilMatch(endTag)) {
	        	Text values = new Text();
	        	values.set(buffer.getData(), 0, buffer.getLength());
	        	
	        	String tempValue =values.toString();
	    		if(startElement==0 && tempValue.contains("<?") && tempValue.contains("?>")){
	    			String element=new String(endTag, "utf-8").replace("</", "<");
	    			try{
	    		    element=element.replace(">", " ");
	    			tempValue=tempValue.substring(tempValue.indexOf(element), tempValue.length());
	    			}catch(Exception e1){
	    			element=element.trim();
	    	    	tempValue=tempValue.substring(tempValue.indexOf(element), tempValue.length());
	    			}
	    		}
	    		startElement++;
	        	try {
	        		XmlParser parser=new XmlParser(tempValue);
	        		String str=parser.getValue().trim();
	        		str=str.substring(0, str.length()-2);
	        		value.set(str);
	        		key.set(fsin.getPos());
				} catch (ParserConfigurationException | SAXException e) {
				}
	               return true;
	        }
	      } finally {
	        buffer.reset();
	      }
	  }
	  return false;
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
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		private boolean readUntilMatch(byte[] match)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();

				if (b == -1)
					return false;

			//	if (withinBlock)
					buffer.write(b);
				
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				if (i == 0 && fsin.getPos() >= end)
					return false;
			}
		}

	}
	private static class XmlParser extends DefaultHandler {
		private StringBuilder value;
		private int tagCount;
		private String tmpValue;
		private int counts;
		private StringBuilder str;
		
		

		public XmlParser(String xml) throws ParserConfigurationException,SAXException, IOException {
			value=new StringBuilder();
			InputStream is = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser parser = factory.newSAXParser();
			parser.parse(is, this);
		}
		
		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			int count = attributes.getLength();
			tagCount=0;
			counts=0;
			str=new StringBuilder();
			for (int i = 0; i < count; i++) {
				String val = attributes.getValue(i).trim();
				value.append(val+",");
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			if(counts>0){
				tmpValue=str.toString().replaceAll("\n", "");
			}
			counts=0;
			value.append(tmpValue+",");
			if(tagCount==0){
				value.append(""+",");
			}
		}
		public String getValue(){
			return value.toString();
	
		}
		@Override
		public void characters(char ch[], int start, int length)
				throws SAXException {
			String tmpValues = new String(ch, start, length).trim().replaceAll(",", " ").replaceAll("\n", "");
			if(!(tmpValues.isEmpty())){
			str.append(tmpValues);
			counts++;
			}
			tmpValue=tmpValues;
			tagCount++;
			
		}
	}

}
