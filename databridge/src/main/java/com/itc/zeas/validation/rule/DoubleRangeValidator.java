package com.itc.zeas.validation.rule;

public class DoubleRangeValidator implements DataValidation {
	
	private String ruleName="DoubleRangeValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private String columnValue;
	private boolean isNull=true;
	private String info;


	@Override
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		
		String validationValue = VAttr.getValidationValue();
		columnName=VAttr.getColumnName();
		validationValue=validationValue.replaceAll("\\s+", "");
		
		String ranges[] = new String[2];
		double range[]=new double[2];

		if (!(inputValue == null || "null".equalsIgnoreCase(inputValue)
				|| inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())) {

			isNull = false;
			columnValue=inputValue;
			
			try {
				
				double columnVal = Double.parseDouble(inputValue.trim());
			
				if (validationValue.contains(":")) {
					ranges = validationValue.split(":");
					range[0] = Double.parseDouble(ranges[0]);
					range[1] = Double.parseDouble(ranges[1]);
					if (columnVal >= range[0] && columnVal <= range[1]) {
						return 1;
					}
					info = "value should be between " + range[0] + " to "
							+ range[1];

				} else if (validationValue.contains("<") && validationValue.contains("=")) {
					range[0] = Double.parseDouble(validationValue.substring(2));
					if(columnVal<=range[0]) {
						return 1;
					}
					info = "value should be equal to or less than " + range[0];
				} else if (validationValue.contains("<")) {
					range[0] = Double.parseDouble(validationValue.substring(1));
					if(columnVal<range[0]) {
						return 1;
					}
					info = "value should be  less than " + range[0];
					
				}else if (validationValue.contains(">") && validationValue.contains("=")) {
					range[0] = Double.parseDouble(validationValue.substring(2));
					if(columnVal>=range[0]) {
						return 1;
					}
					info = "value should be equal to or greater than " + range[0];
				} else if (validationValue.contains(">")) {
					range[0] = Double.parseDouble(validationValue.substring(1));
					if(columnVal>range[0]) {
						return 1;
					}
					info = "value should be greater than " + range[0];
				}
				
			return 0;
			} catch (NumberFormatException nfe) {
				info = "expected value should be Double value";
				columnValue=inputValue;
				return 0;
			}
			
		}

		return -1;
	}

	@Override
	public String getError() {
		if(isNull){
			info="validation value should not be null or empty or N/A";
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

