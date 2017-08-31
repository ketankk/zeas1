package com.zdp.transformations.functions;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class GetMeanForCols implements Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2917376616884518867L;

	public Tuple2<Double, Long> call(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
		Double d1 = a._1();
		Double d2 = b._1();
		Long l1 = a._2();
		Long l2 = b._2();
		return new Tuple2<Double, Long>(d1 + d2, l1 + l2);
	}
}
