package com.itc.zeas.validation.rule;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateRangeValidator implements DataValidation {
	
	private String ruleName="DateRangeValidator";
	private String columnName;
	private StringBuilder errorMessage;
	private Date columnValue;
	private boolean isNull=true;
	private String info;
	private boolean isDate;
	private String inptValue;

	@Override
	public int isValidate(ValidationAttribute validation, String inputValue) {
		
		String validationValue = validation.getValidationValue();
		columnName=validation.getColumnName();
		SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
		
		String range[] = validationValue.split(",");
		
		if(!(inputValue==null || "null".equalsIgnoreCase(inputValue) || inputValue.equalsIgnoreCase("na") || inputValue.isEmpty())){
			
			isNull = false;
			inptValue=inputValue;
			String separator = String.valueOf(inputValue.charAt(4));
			String[] value = inputValue.split(separator);
			if (checkDate(value) && checkDate(range[0].split(separator))
					&& checkDate(range[1].split(separator))) {
				isDate = true;
			} else {
				isDate = false;
				info = "wrong day or month date should be in proper format";
			}
			inputValue = inputValue.replace(separator, "-");
			range[0] = range[0].replace(separator, "-");
			range[1] = range[1].replace(separator, "-");

			if (isDate) {
				try {
					columnValue = formatter.parse(inputValue);
					if (columnValue.before(formatter.parse(range[0]))
							|| columnValue.after(formatter.parse(range[1]))) {

						info = "range should be between " + range[0] + " to "
								+ range[1];

						return 0;
					} else{
						return 1;
					}
				} catch (ParseException e) {
					info = "Value should be in proper format" + e.toString();
					System.out.println("Parsing failed" + e.toString());
					return 0;
				}
			} else{
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
		errorMessage.append(inptValue);
		errorMessage.append(",");
		errorMessage.append(columnName);

		return errorMessage.toString();
	}
	
	@Override
	public String toString() {
		
		return "Rule Name is "+ruleName+ " in the column "+columnName + " with value "+ inptValue;
	}

	@Override
	public String performMasking(ValidationAttribute VAttr, String inputValue) {
		
		return null;
	}
	
	private boolean checkDate(String[] values){
		
		try {
			if (values[1].equals("0") || values[2].equals("0")
					|| Integer.parseInt(values[1]) > 12
					|| Integer.parseInt(values[2]) > daysInMonth(Integer.parseInt(values[0]), Integer.parseInt(values[1]))) {
				return false;
			}
		} catch (NumberFormatException e3) {
			info = "day or month should be integer";
			return false;
		}
		return true;
	}
	private static int daysInMonth(int year, int month) {
	    int daysInMonth;
	    switch (month) {
	        case 1: 
	        case 3: 
	        case 5: 
	        case 7: 
	        case 8: 
	        case 10: 
	        case 12:
	            daysInMonth = 31;
	            break;
	        case 2:
	            if (((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0)) {
	                daysInMonth = 29;
	            } else {
	                daysInMonth = 28;
	            }
	            break;
	        default:
	            daysInMonth = 30;
	    }
	    return daysInMonth;
	}
}
