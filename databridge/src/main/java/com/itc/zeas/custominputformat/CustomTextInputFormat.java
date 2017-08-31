package com.itc.zeas.custominputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * it's a custom input format format capable of handling scenario where first
 * record is Header
 * 
 * @author 19217
 * 
 */
public class CustomTextInputFormat extends TextInputFormat {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new CustomRecordReader();
	}
}
