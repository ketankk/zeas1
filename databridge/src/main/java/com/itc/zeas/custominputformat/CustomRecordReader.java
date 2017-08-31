package com.itc.zeas.custominputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

/**
 * 
 * @author 19217
 * 
 */
public class CustomRecordReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long pos;
	private long end;
	private SplitLineReader splitLineReader;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	/* it's a flag used for handling First record header scenario */
	private boolean isFirstRecordHeader;
	private byte[] recordDelimiterBytes = null;

	private static final Log LOG = LogFactory.getLog(CustomRecordReader.class);

	public CustomRecordReader() {
	}

	public CustomRecordReader(byte[] recordDelimiter) {
		this.recordDelimiterBytes = recordDelimiter;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		LOG.debug("initializing the file split");
		// This InputSplit is a FileInputSplit
		FileSplit split = (FileSplit) genericSplit;

		// Retrieve configuration, and Max allowed
		// bytes for a single record
		Configuration jobConf = context.getConfiguration();
		this.maxLineLength = jobConf.getInt(
				"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		isFirstRecordHeader = jobConf.getBoolean("first.record.header",
				Boolean.FALSE);
		// Split "S" is responsible for all records
		// starting from "start" and "end" positions
		start = split.getStart();
		end = start + split.getLength();

		// Retrieve file containing Split "S"
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(jobConf);
		FSDataInputStream fileIn = fs.open(split.getPath());

		// If Split "S" starts at byte 0, first line will be processed
		// If Split "S" does not start at byte 0, first line has been already
		// processed by "S-1" and therefore needs to be silently ignored
		boolean skipFirstLine = false;
		if (start != 0) {
			LOG.debug("file split does not include begining of file");
			skipFirstLine = true;

			// Set the file pointer at "start - 1" position.
			// This is to make sure we won't miss any line
			// It could happen if "start" is located on a EOL
			--start;
			fileIn.seek(start);
		} else if (isFirstRecordHeader) {
			// first record is header and split contains beginning of file
			LOG.debug("first record is header and split contains beginning of file");
			skipFirstLine = true;
		}

		splitLineReader = new SplitLineReader(fileIn, jobConf,
				recordDelimiterBytes);

		// If first line needs to be skipped, read first line
		// and stores its content to a dummy Text
		if (skipFirstLine) {
			Text textToBeDiscarded = new Text();
			// Reset "start" to "start + line offset"
			start += splitLineReader.readLine(textToBeDiscarded);
			LOG.debug("textToBeDiscarded: " + textToBeDiscarded);
		}

		// Position is the actual start
		this.pos = start;

	}

	/**
	 * From Design Pattern, O'Reilly... Like the corresponding method of the
	 * InputFormat class, this reads a single key/ value pair and returns true
	 * until the data is consumed.
	 */
	@Override
	public boolean nextKeyValue() throws IOException {

		// Current offset is the key
		key.set(pos);
		LOG.info("nextKeyValue: ::::::::::::::::::::::::::::::::::::::::::::::::::::::::pos:"
				+ pos);
		int newSize = 0;

		// Make sure we get at least one record that starts in this Split
		while (pos < end) {

			// Read first line and store its content to "value"
			newSize = splitLineReader.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));

			// No byte read, seems that we reached end of Split
			// Break and return false (no key / value)
			if (newSize == 0) {
				break;
			}

			// Line is read, new position is set
			pos += newSize;

			// Line is lower than Maximum record line size
			// break and return true (found key / value)
			if (newSize < maxLineLength) {
				break;
			}

			// Line is too long
			// Try again with position = position + line offset,
			// i.e. ignore line and go to next one
			// TODO: Shouldn't it be LOG.error instead ??
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}

		if (newSize == 0) {
			// We've reached end of Split
			key = null;
			value = null;
			return false;
		} else {
			// Tell Hadoop a new line has been found
			// key / value will be retrieved by
			// getCurrentKey getCurrentValue methods
			return true;
		}
	}

	/**
	 * From Design Pattern, O'Reilly... This methods are used by the framework
	 * to give generated key/value pairs to an implementation of Mapper. Be sure
	 * to reuse the objects returned by these methods if at all possible!
	 */
	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	/**
	 * From Design Pattern, O'Reilly... This methods are used by the framework
	 * to give generated key/value pairs to an implementation of Mapper. Be sure
	 * to reuse the objects returned by these methods if at all possible!
	 */
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	/**
	 * From Design Pattern, O'Reilly... Like the corresponding method of the
	 * InputFormat class, this is an optional method used by the framework for
	 * metrics gathering.
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	/**
	 * From Design Pattern, O'Reilly... This method is used by the framework for
	 * cleanup after there are no more key/value pairs to process.
	 */
	@Override
	public void close() throws IOException {
		if (splitLineReader != null) {
			splitLineReader.close();
		}
	}

}