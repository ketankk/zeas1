package com.zdp.transformations.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class FilterRequiredColumnsForMedian implements FlatMapFunction<String, Tuple2<Tuple2<String, Double>, String>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -506833741889383515L;

	private String[] indexArr;

	public FilterRequiredColumnsForMedian(String[] indexArr) {
		this.indexArr = indexArr;
	}

	public String[] getIndexArr() {
		return indexArr;
	}

	public void setIndexArr(String[] indexArr) {
		this.indexArr = indexArr;
	}

	public Iterable<Tuple2<Tuple2<String, Double>, String>> call(String s) {
		List<Tuple2<Tuple2<String, Double>, String>> list = new ArrayList<Tuple2<Tuple2<String, Double>, String>>();
		String[] val = s.split(",", -1);
		for (String in : indexArr) {
			int i = new Integer(in);
			if (!(null == val[i] || val[i].isEmpty() || val[i].equalsIgnoreCase("NA")
					|| val[i].equalsIgnoreCase("NULL"))) {
				list.add(new Tuple2<Tuple2<String, Double>, String>(
						new Tuple2<String, Double>(in, Double.parseDouble(val[i].trim())), null));
			}
		}
		return list;
	}
}
