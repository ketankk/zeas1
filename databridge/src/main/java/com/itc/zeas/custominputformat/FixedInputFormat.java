package com.itc.zeas.custominputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.itc.zeas.custominputformat.XmlInputFormat.XmlRecordReader;

/**
 * 
 * Description: Input Format to Read the Fixed Length Files
 * 
 * @author: nisith.nanda
 * 
 *          Version:
 * 
 *          Date: 25-Aug-2015
 */
public class FixedInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new FixedRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	public static class FixedRecordReader extends RecordReader<LongWritable, Text> {
		private static final Logger LOG = LoggerFactory.getLogger(FixedRecordReader.class);

		// added by Deepak to handle First record Header scenario START
		private CustomRecordReader customRecordReader = new CustomRecordReader();

		private final Text value_ = new Text();
		private static String[] fixedFieldLengths;
		private static final String FIXED_FIELD_LENGTHS = "fixedValues";
		public static final Log log = LogFactory.getLog(XmlRecordReader.class);

		public FixedRecordReader() {
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

			LOG.info("Initialize Method of FixedRecordReader started");
			fixedFieldLengths = context.getConfiguration().get(FIXED_FIELD_LENGTHS).split(",");
			customRecordReader.initialize(split, context);
			LOG.info("Initialize Method of FixedRecordReader ended");

		}

		@Override
		public synchronized void close() throws IOException {
			customRecordReader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return customRecordReader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value_;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return customRecordReader.getProgress();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			LOG.info("nextKeyValue Method of FixedRecordReader started");
			while (customRecordReader.nextKeyValue()) {
				value_.clear();
				if (fixedInputSeprator(customRecordReader.getCurrentValue(), value_)) {
					return true;
				}
			}
			LOG.info("nextKeyValue Method of FixedRecordReader ended");
			return false;
		}

		public static boolean fixedInputSeprator(Text line, Text value) {
			log.info("Got the Input line");
			//System.out.println(line.toString());

			try {
				String val = line.toString();
				int i, l, k;
				i = l = k = 0;
				String tempVal = null;
				for (String str : fixedFieldLengths) {

					i = Integer.parseInt(str);
					String strout = val.toString().substring(l, i + k);
					if (null == tempVal) {
						tempVal = strout.trim();
					} else {
						tempVal = tempVal + "," + strout.trim();
					}
					l = i + l;
					k = k + i;
				}
				value.set(tempVal);
				return true;
			} catch (Exception e) {
				LOG.warn("Could not separate the line: " + line, e);
				throw e;
			}
		}

	}
}
