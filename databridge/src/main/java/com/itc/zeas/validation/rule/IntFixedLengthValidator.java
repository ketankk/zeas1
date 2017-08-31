package com.itc.zeas.validation.rule;

public class IntFixedLengthValidator implements DataValidation {
	
	private String ruleName="IntFixedLengthValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue;
	private String info;
	private boolean isNull=true;
	
	@Override
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		
		String validationValue = VAttr.getValidationValue();
		
		columnName=VAttr.getColumnName();
		
		if(!(inputValue==null || "null".equalsIgnoreCase(inputValue) || inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())){
			isNull=false;
			columnValue = inputValue;
			
			try {
				if (columnValue.trim().length() != Integer.parseInt(validationValue.trim())) {
					info=" length should be " + validationValue;

					return 0;
				} else
					return 1;
			} catch (NumberFormatException nfe) {
				info="expected value should be Integer";
				return 0;
			}
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
