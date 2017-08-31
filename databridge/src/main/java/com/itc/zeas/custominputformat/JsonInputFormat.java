package com.itc.zeas.custominputformat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.itc.zeas.validation.rule.JSONDataParser;
import com.itc.zeas.validation.rule.JsonColumnValidatorParser;
import com.itc.zeas.validation.rule.ValidationAttribute;

/**
 * Assumes one line per JSON object
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

	private static JobContext context;

	private static final Logger log = LoggerFactory
			.getLogger(JsonInputFormat.class);

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		this.context = context;
		log.info("reading RecoredReader");
		return new JsonRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	public static class JsonRecordReader extends
			RecordReader<LongWritable, Text> {

		private static final Logger LOG = LoggerFactory
				.getLogger(JsonRecordReader.class);

		private LineRecordReader reader = new LineRecordReader();

		private final Text currentLine_ = new Text();
		private final Text value_ = new Text();

		private final JSONParser jsonParser_ = new JSONParser();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			log.info("initializing");
			reader.initialize(split, context);
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
			return value_;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()) {
				value_.clear();

				if (decodeLineToJson(jsonParser_, reader.getCurrentValue(),
						value_)) {
					return true;
				}
			}
			return false;
		}

		public static boolean decodeLineToJson(JSONParser parser, Text line,
				Text value) {
			Map<String, String> columnNameAndDataType;
			Configuration conf = context.getConfiguration();
			JSONDataParser dataTypeparser = new JSONDataParser();
			// Map<Integer, String> dataType;
			String json = conf.get("dataSchema.JSON");
			JsonColumnValidatorParser attrParser = new JsonColumnValidatorParser();
			Map<Integer, List<ValidationAttribute>> tmpColValidatorMap = attrParser
					.JsonParser(json);
			// dataType = DataTypeCheckUtility
			// .getcolNumberAndDataTypeMap(columnNameAndDataType);
			dataTypeparser.JsonParser(json);
			List<String> columnNameList = dataTypeparser.getColumnList();
			/*for (Entry<String, String> entry : columnNameAndDataType.entrySet()) {
				log.info("entry.getKey col name: " + entry.getKey()
						+ "entry.getValue col dataType: " + entry.getValue());
				columnNameList.add(entry.getKey());
			}*/

			// /
			JSONObject jsonObj = null;
			line = new Text(line.toString().trim());
			int len = line.toString().length();
			try {
				if (!(line.toString().contains("{") && line.toString()
						.contains("}"))) {
					log.info("");
					// line will be excluded
					return false;
				}
				// line contains ',' will be parsing into json parser
				else if (line.toString().substring(len - 1).contains(",")) {
					jsonObj = (JSONObject) parser.parse(line.toString()
							.substring(0, len - 1));
				} else {
					// parse the lines if dont contain "," at end of record
					jsonObj = (JSONObject) parser.parse(line.toString()
							.substring(0, len));
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String temp = "";
			String mapValue = "";
			for (String key : columnNameList) {
				Object valObject = jsonObj.get(key);
				if (valObject != null) {
					mapValue = valObject.toString();
				} else {
					mapValue = null;
				}
				temp = temp + "," + mapValue;
			}
			Text finalvalue = new Text(temp.substring(1));
			// setting value as input to mapper for each record
			value.set(finalvalue);
			return true;
			// //


		}
	}
}
