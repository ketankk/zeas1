package com.itc.zeas.validation.rule;

public class StringRangeValidator implements DataValidation {
	
	private String ruleName="StringRangeValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue ;
	private boolean isNull=true;
	private String info;

	@Override
	public int isValidate(ValidationAttribute validation, String inputValue) {
		
		String validationValue = validation.getValidationValue();
		columnName=validation.getColumnName();
		validationValue=validationValue.replaceAll("\\s+", "");
		String ranges[] = new String[2];
		long range[]=new long[2];

		if (!(inputValue == null || "null".equalsIgnoreCase(inputValue)
				|| inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())) {

			isNull = false;

			try {
				columnValue=inputValue;
				if (validationValue.contains(":")) {
					ranges = validationValue.split(":");
					range[0] = Long.parseLong(ranges[0]);
					range[1] = Long.parseLong(ranges[1]);
					if (columnValue.length() > range[0] && columnValue.length() < range[1]) {
						return 1;
					}
					info = "String value length should be between " + range[0] + " to "
							+ range[1];

				} else if (validationValue.contains("<") && validationValue.contains("=")) {
					range[0] = Long.parseLong(validationValue.substring(2));
					if(columnValue.length()<=range[0]) {
						return 1;
					}
					info = "String value length should be equal to or less than " + range[0];
				} else if (validationValue.contains("<")) {
					range[0] = Long.parseLong(validationValue.substring(1));
					if(columnValue.length()<range[0]) {
						return 1;
					}
					info = "String value length should be  less than " + range[0];
					
				}else if (validationValue.contains(">") && validationValue.contains("=")) {
					range[0] = Long.parseLong(validationValue.substring(2));
					if(columnValue.length()>=range[0]) {
						return 1;
					}
					info = "String value length should be equal to or greater than " + range[0];
				} else if (validationValue.contains(">")) {
					range[0] = Long.parseLong(validationValue.substring(1));
					if(columnValue.length()>range[0]) {
						return 1;
					}
					info = "String value length should be greater than " + range[0];
				}
				
				return 0;
			} catch (NumberFormatException nfe) {
				info = "expected validation value is a integer";
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
