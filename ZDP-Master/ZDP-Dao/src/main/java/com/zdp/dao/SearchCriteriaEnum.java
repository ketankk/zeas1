package com.zdp.dao;




/**
 * Description: This enum class helps to make search criteria which is compatible with oracle equivalent search criteria type.
 * 
 */
public enum SearchCriteriaEnum {
	EQUALS("="), NOT_EQUAL("<>"), GREATER_THAN(">"), LESSER_THAN("<"), CONTAINS("LIKE"), GREATER_THAN_OR_EQUAL(
			">="), LESSER_THAN_OR_EQUAL("<="), IN("IN"), IS("IS"), IS_NOT("IS NOT");

	private String searchType;

	SearchCriteriaEnum(String searchType) {

		this.searchType = searchType;
	}

	/**
	 * 
	 * Description: This method returns the oracle equivalent search criteria type.
	 * 
	 * 
	 * @return
	 */
	public String getCriteriaType() {

		return this.searchType;
	}
}
