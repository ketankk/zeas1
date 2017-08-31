package com.zdp.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.itc.zeas.project.extras.QueryConstants;
import com.itc.zeas.project.model.SearchCriterion;

public class WhereClauseQueryFormatioin {

	StringBuilder strBlrQuery = new StringBuilder(); // String Builder to build the query
	List<String> passedValues = new ArrayList<String>(); // Values that are to be assigned to the prepared statement

	
	public Map<String,List<String>> getWhereClause(List<SearchCriterion> searchCriterions) throws Exception {
		
		
		Map<String,List<String>> columnQueryAndColumnValues = new HashMap<String, List<String>>();
		if(searchCriterions !=null && searchCriterions.size()>0){
			strBlrQuery.append(" where ");
		
		//if ( searchCriterions != null ) {
			int index=1;
			for ( SearchCriterion criterion : searchCriterions ) {
				
					String queryCombinationType = criterion.getQueryCombinationType();
					String strCloseValue = null;
					if ( queryCombinationType.equalsIgnoreCase ( QueryConstants.QUERY_AND_CLOSE_TYPE ) ) {
						queryCombinationType = QueryConstants.QUERY_AND_TYPE;
						strCloseValue = " ) ";
					}
					else if ( queryCombinationType
							.equalsIgnoreCase ( QueryConstants.QUERY_OR_CLOSE_TYPE ) ) {
						queryCombinationType = QueryConstants.QUERY_OR_TYPE;
						strCloseValue = " ) ";
					}
					
					else if ( queryCombinationType
							.equalsIgnoreCase ( QueryConstants.QUERY_OPEN_TYPE ) ) {
						queryCombinationType = QueryConstants.QUERY_OPEN_TYPE;
						//strCloseValue = " ( ";
					}
					
					
					if(index !=1){
					strBlrQuery.append ( " " + queryCombinationType + " " );
					}
					else if(index==1 && queryCombinationType
							.equalsIgnoreCase ( QueryConstants.QUERY_OPEN_TYPE )) {
						strBlrQuery.append ( " " + queryCombinationType + " " );
					}
					index++;
					this.addWhereConditionForOtherObject ( criterion );
					if ( strCloseValue != null ) {
						strBlrQuery.append ( strCloseValue );
					}
				
			}
		//}
		}
		
		List<String> query= new ArrayList<>();
		query.add(strBlrQuery.toString());
		columnQueryAndColumnValues.put(QueryConstants.columnQuery, query);
		columnQueryAndColumnValues.put(QueryConstants.columnValues, passedValues);
		return columnQueryAndColumnValues;
				
	}
	
	
	private void addWhereConditionForOtherObject(SearchCriterion criterion) throws Exception {

		String columnName = criterion.getKey ();
		String oracleEqlSearchCriteriaType = criterion.getSearchCriteriaType().getCriteriaType ();
		String strValue = criterion.getValue ();
		strBlrQuery.append ( columnName );
		if ( strValue != null ) {
			strBlrQuery.append ( " " + oracleEqlSearchCriteriaType + " ? " );
			if ( SearchCriteriaEnum.CONTAINS.equals ( criterion.getSearchCriteriaType() ) ) {
				strValue = strValue.replace ( '*', '%' );
			}
			passedValues.add ( strValue );
		}
		else {
			strBlrQuery.append ( " IS NULL " );
		}
	}
	
	

}
