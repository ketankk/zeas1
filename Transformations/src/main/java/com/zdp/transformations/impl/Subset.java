package com.zdp.transformations.impl;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * This class will get the input dataset and resulted subset(Part of Input data) will be saved as new dataset.
 * type of subset(Random_sampling/head) and % of data will be passed as an arguement.
 * @author 19173
 */
public class Subset extends AbstractTransformations implements Serializable {
	private static final long serialVersionUID = 1505583711142632221L;
	Logger logger = Logger.getLogger(Subset.class);
	String[] args;

	public Subset(String[] arg) {
		args = arg;
	}

	public void execute() {

		try {
			// args[0] - type of transformation
			// args[1] - schema
			// args[2] - InputPath ; OutputPath; % of data; randomSampling/head
			// iopath[0] - inputhPath
			// iopath[1] - outputPath
			// params[0] - randomSampling/head
			// params[1] - fraction in %

			String[] ioPath = args[2].split(";");

			Double fraction = Double.parseDouble(ioPath[2]) / 100;
			logger.info("fraction is " + fraction);
			JavaSparkContext sc = new JavaSparkContext();

			JavaRDD<String> inputDataRDD = sc.textFile(ioPath[0]);
			if (ioPath[3].equalsIgnoreCase("Random_Sampling")) {
				JavaRDD<String> outputRDD = inputDataRDD.sample(true, fraction);
				logger.info("write results to ouptput path");
				outputRDD.saveAsTextFile(ioPath[1]);
			} else if (ioPath[3].equalsIgnoreCase("Head"))
			{
				logger.info("value in take is " + (int) (fraction * (double) inputDataRDD.count()));
				logger.info(inputDataRDD.count());
				List<String> result = inputDataRDD.take((int) (fraction * (double) inputDataRDD.count()));

				sc.parallelize(result).saveAsTextFile(ioPath[1]);

			}
			sc.close();
		}

		catch (Exception e) {
			e.printStackTrace();
		}

	}

}
