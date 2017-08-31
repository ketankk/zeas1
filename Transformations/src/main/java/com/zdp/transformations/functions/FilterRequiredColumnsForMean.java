package com.zdp.transformations.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class FilterRequiredColumnsForMean implements FlatMapFunction<String, Tuple2<String, Tuple2<Double, Long>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8842508422288895443L;
	private String[] indexArr;

	public FilterRequiredColumnsForMean(String[] indexArr) {
		this.indexArr = indexArr;
	}

	public String[] getIndexArr() {
		return indexArr;
	}

	public void setIndexArr(String[] indexArr) {
		this.indexArr = indexArr;
	}

	public Iterable<Tuple2<String, Tuple2<Double, Long>>> call(String s) {
		List<Tuple2<String, Tuple2<Double, Long>>> list = new ArrayList<Tuple2<String, Tuple2<Double, Long>>>();
		String[] val = s.split(",", -1);
		for (String in : indexArr) {
			int i = new Integer(in);
			if (!(null == val[i] || val[i].isEmpty() || val[i].equalsIgnoreCase("NA")
					|| val[i].equalsIgnoreCase("NULL"))) {
				list.add(new Tuple2<String, Tuple2<Double, Long>>(in,
						new Tuple2<Double, Long>(Double.parseDouble(val[i].trim()), new Long(1))));
			}
		}
		return list;
	}
}
