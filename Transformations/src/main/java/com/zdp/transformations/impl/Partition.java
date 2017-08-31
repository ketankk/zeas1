package com.zdp.transformations.impl;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Partition extends AbstractTransformations implements Serializable {
	/**
	 * this class will partition the data and save resulted partitions to new datasets.
	 * We will pass partition Percentage as an arguement .
	 */
	private static final long serialVersionUID = -2234001621125136961L;
	Logger logger = Logger.getLogger(Partition.class);
	String[] args;

	public Partition(String[] arg) {
		args = arg;
	}

	public void execute() {
		// args[0] - type of transformation
		// args[1] - schema
		// args[2] - input data; output paths(,); % of data
		// ioPath[0] - inputPath
		// ioPath[1] - output paths(comma separated)
		// params[0] - fraction in %
		try {
			String[] ioPath = args[2].split(";");
			logger.info("arguements for Partition class" + ioPath + " ,  " + ioPath[2]);

			String[] PartitionDataPath = ioPath[1].split(",");

			if (ioPath[1] == null || PartitionDataPath.length < 2) {
				System.out.println("Locations for saving partitioned datsets not passed");

			}
			JavaSparkContext sc = new JavaSparkContext();
			JavaRDD<String> data = sc.textFile(ioPath[0]);
			JavaRDD<String>[] partitions = data.randomSplit(
					new double[] { Double.parseDouble(ioPath[2]) / 100, 1 - Double.parseDouble(ioPath[2]) / 100 });
			partitions[0].saveAsTextFile(PartitionDataPath[0]);
			partitions[1].saveAsTextFile(PartitionDataPath[1]);
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
