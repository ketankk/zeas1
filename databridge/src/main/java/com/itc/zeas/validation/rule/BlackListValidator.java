package com.itc.zeas.validation.rule;

public class BlackListValidator implements DataValidation {
	
	private String ruleName="BlackListValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue;
	private boolean isNull=true;
	private String info;

	@Override
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		
		String validationValue = VAttr.getValidationValue();
		
		columnName=VAttr.getColumnName();
		String blackListValues[]=validationValue.split(",");
		String values=validationValue.replace(",", "|");
		
		boolean ifFound=false;
		
		if(!(inputValue==null || "null".equalsIgnoreCase(inputValue) || inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())){
			
			isNull=false;
			columnValue = inputValue;
			
			for(String s : blackListValues){
				if(columnValue.equalsIgnoreCase(s)){
					ifFound=true;
					break;
				}
			}
		
		if (ifFound) {
			info="expected value should not be among this list "+values;
				
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
