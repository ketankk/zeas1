package com.itc.zeas.validation.rule;

public class NotNullValidator implements DataValidation{
	
	private String ruleName="NotNullValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue;
	private boolean isNull=true;
	private String info="";

	@Override
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		
		String validationValue = VAttr.getValidationValue();
		columnName=VAttr.getColumnName();
	    columnValue= inputValue;
		if (validationValue.equalsIgnoreCase("yes")) {
			
			/*isNull=false;
			
			info="value should not be NULL or empty or NA";*/
			return 1;
		}

		return 0;
	}

	@Override
	public String getError() {
		if(isNull){
			info="validation value should not be null";
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
		return null;
	}
}
