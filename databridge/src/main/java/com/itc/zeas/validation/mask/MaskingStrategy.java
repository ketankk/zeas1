package com.itc.zeas.validation.mask;

public interface MaskingStrategy {
	/**
	 * Gives masked data for given input
	 * 
	 * @param dataToBeMasked
	 *            input data that to be masked
	 * @param dataCategory
	 *            category of input data to be masked
	 * @return string representing masked output
	 */
	String mask(String dataToBeMasked, String dataCategory);
}
