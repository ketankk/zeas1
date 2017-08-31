package com.itc.zeas.custominputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Description: Input Format to Read the Delimited Files
 * 
 * @author: nisith.nanda
 * 
 *          Version:
 * 
 *          Date: 25-Aug-2015
 */
public class DelimitedFileInputFormat extends
		FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new DelimitedFileRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	public static class DelimitedFileRecordReader extends
			RecordReader<LongWritable, Text> {
		private static final Logger LOG = LoggerFactory
				.getLogger(DelimitedFileRecordReader.class);

		private static final String COlUMN_DELIMITER = "colDeli";

		private static final String ROW_DELIMITER = "rowDeli";

		// added by Deepak to handle First record Header scenario START
		private CustomRecordReader reader = new CustomRecordReader();

		private final Text value = new Text();

		private Map<String, String> delimiters = new HashMap<String, String>();

		private String column_delim;
		private String row_delim;

		public DelimitedFileRecordReader() {

		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			LOG.info("Initialize Method of DelimitedFileRecordReader started");
			column_delim = context.getConfiguration().get(COlUMN_DELIMITER);
			row_delim = context.getConfiguration().get(ROW_DELIMITER);
			// System.out.println(" Test  -- " + row_delim);
			this.delimiters.put("tab", "\t");
			this.delimiters.put("WhiteSpace", "\\s+");
			this.delimiters.put("space", " ");
			this.delimiters.put("comma", ",");
			this.delimiters.put("underscore", "_");
			this.delimiters.put("slash", "-");
			this.delimiters.put("Control-A", "\\^A");
			this.delimiters.put("Control-B", "\\^B");
			this.delimiters.put("Control-C", "\\^C");
			this.delimiters.put("newline", "\n");
			this.delimiters.put("carriage return", "\r\n");
			this.column_delim = this.delimiters.get(this.column_delim
					.toString());
			this.row_delim = this.delimiters.get(this.row_delim.toString());
			/*
			 * System.out.println("Row Deli : " + this.row_delim.toString());
			 * System.out.println("Col Deli : " + this.column_delim);
			 */this.reader = new CustomRecordReader(row_delim.getBytes());
			this.reader.initialize(split, context);
			LOG.info("Initialize Method of DelimitedFileRecordReader ended");
		}

		@Override
		public synchronized void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			LOG.info("nextKeyValue Method of DelimitedFileRecordReader started");
			while (reader.nextKeyValue()) {
				value.clear();
				String line = reader.getCurrentValue().toString().trim();
				String[] vals = line.split(column_delim, -1);
				String tempVal = null;
				for (String val : vals) {
					if (null == tempVal) {
						tempVal = val;
					} else {
						tempVal = tempVal + "," + val;
					}
				}
				value.set(tempVal);
				return true;

			}
			LOG.info("Initialize Method of DelimitedFileRecordReader ended");
			return false;
		}
	}
}
