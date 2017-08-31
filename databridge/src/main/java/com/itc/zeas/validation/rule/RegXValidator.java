package com.itc.zeas.validation.rule;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


public class RegXValidator implements DataValidation {
	
	private String ruleName="RegXValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue;
	private boolean isNull=true;
	private String info="";

	@Override
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		
		String validationValue = VAttr.getValidationValue();
		
		
		columnName=VAttr.getColumnName();
		
		boolean ifMatch = false;
		
		if(!(inputValue==null || "null".equalsIgnoreCase(inputValue) || inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())){
			isNull=false;
			columnValue = inputValue;
		
			 // Create a Pattern object
			Pattern r=null;
			try {
				r = Pattern.compile(validationValue, Pattern.CASE_INSENSITIVE);
			} catch (PatternSyntaxException e) {
				info = "Pattern should be in correct format";
				return 0;
			}
		      // Now create matcher object.
		      Matcher m = r.matcher(columnValue);
		      
		      //check whether match found or not
		      if (m.matches()) {
		        // System.out.println("Match Found  ");
		         ifMatch=true;
		      } 
			
		if (!ifMatch) {
			info =" value should match with pattern "+validationValue;
			
			return 0;
		}	
		else return 1;
	}
		
		return -1;
	}

	@Override
	public String getError() {
		if(isNull){
			info="validation value should not be null or empty or N/A";
			columnValue="null";
		}
		errorMessage=new StringBuilder();
		errorMessage.append(ruleName);
		errorMessage.append(",");
		errorMessage.append(info);
		errorMessage.append(",");
		errorMessage.append(columnValue);
		errorMessage.append(",");
		errorMessage.append(columnName);
		
		return errorMessage.toString();
	}
	
	@Override
	public String toString() {
		
		return "Rule Name is "+ruleName+ " in the column "+columnName + " with value "+ columnValue;
	}

	@Override
	public String performMasking(ValidationAttribute VAttr, String inputValue) {
		// TODO Auto-generated method stub
		return null;
	}
}

