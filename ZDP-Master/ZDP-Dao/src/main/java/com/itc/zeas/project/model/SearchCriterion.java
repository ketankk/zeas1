package com.itc.zeas.project.model;

import com.itc.zeas.project.extras.QueryConstants;
import com.zdp.dao.SearchCriteriaEnum;
import lombok.Data;

@Data
public class SearchCriterion {

    String key;
    String value;
    SearchCriteriaEnum searchCriteriaType;
    String queryCombinationType ;

    public SearchCriterion(String key, String value, SearchCriteriaEnum searchCriteriaType) {

        this.key = key;
        this.searchCriteriaType = searchCriteriaType;
        this.value = value;
        this.queryCombinationType=QueryConstants.QUERY_AND_TYPE;
    }
    
    
    public SearchCriterion(String key, String value, SearchCriteriaEnum searchCriteriaType, String queryCombinationType){
    	this.key=key;
    	this.searchCriteriaType=searchCriteriaType;
    	this.value=value;
    	this.queryCombinationType=queryCombinationType;
    	
    }


    
    @Override
    public boolean equals(Object obj) {

        boolean equals = false;
        
        if(null == obj){
        	return false;
        }
      //checking for comparison as it shown type test need to perform
        if(this == obj){
        	return true;
        }
        if(obj instanceof SearchCriterion) {
        SearchCriterion criterion = (SearchCriterion) obj;
        if (criterion.getKey().equalsIgnoreCase(this.getKey()) && criterion.getValue().equalsIgnoreCase(this.getValue())
                && criterion.getSearchCriteriaType() == this.getSearchCriteriaType()) {
            equals = true;
        }
        }
        return equals;

    }

    @Override
    public int hashCode() {
        int hashCode = this.key.hashCode() + this.value.hashCode() + ( this.searchCriteriaType.hashCode());
        return 39 * hashCode;
    }
}

