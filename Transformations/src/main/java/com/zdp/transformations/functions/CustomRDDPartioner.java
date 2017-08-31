package com.zdp.transformations.functions;

import java.util.Map;


import org.apache.spark.Partitioner;

import scala.Tuple2;

/**
 * @author nisith.nanda
 * @since 09-Sep-2015
 */
public class CustomRDDPartioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5341314089549873412L;
	private int numParts;
	private Map<String, Integer> indxPartnMap;

	public CustomRDDPartioner() {

	}

	public CustomRDDPartioner(int numParts, Map<String, Integer> indxPartnMap) {
		this.numParts = numParts;
		this.indxPartnMap = indxPartnMap;
	}

	@Override
	public int numPartitions() {
		return numParts;
	}

	@SuppressWarnings("unchecked")
	public int getPartition(Object key) {
		Tuple2<String, Double> tuple = (Tuple2<String, Double>) key;
		return indxPartnMap.get(tuple._1());
	}
}
