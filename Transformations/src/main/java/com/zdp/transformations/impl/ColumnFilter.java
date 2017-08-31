package com.zdp.transformations.impl;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.zdp.transformations.functions.FilterColumns;

public class ColumnFilter extends AbstractTransformations implements Serializable {
	/**
	 * This class will filter the columns and save those columns to new output.
	 * we will pass Column indexes as an arguement .
	 */
	private static final long serialVersionUID = -1222089725346619208L;
	Logger logger = Logger.getLogger(ColumnFilter.class);
	public String[] args;

	public ColumnFilter(String[] arg) {
		args = arg;
	}

	public void execute() {
		// args[0] - type of transformation
		// args[0] - schema
		// args[2] - inputh path; output path ; column indexes
		// ioPath[0] - inputPath
		// ioPath[1] - outputPath
		// ioPath[2] - columnIndexes

		try {
			String[] ioPath = args[2].split(";");
			logger.info("arguements for columnFilter  " + args[2]);
			JavaSparkContext sc = new JavaSparkContext();
			// SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
			JavaRDD<String> inputDataRDD = sc.textFile(ioPath[0]);

			String[] columnIndexes = ioPath[2].split(",");
			final int[] columnIndex = new int[columnIndexes.length];
			for (int i = 0; i < columnIndexes.length; i++) {
				columnIndex[i] = Integer.parseInt(columnIndexes[i]);
			}

			JavaRDD<String> resultData = inputDataRDD.map(new FilterColumns(columnIndex));
			resultData.saveAsTextFile(ioPath[1]);
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
