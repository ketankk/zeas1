package com.itc.zeas.validation.rule;

public class DataValidatorFactory  {

	public static DataValidation getValidationInstance(String validationName) {

		DataValidation dataValidation = null;
		switch (validationName) {
			case "IntFixedLenghtValidator":
				dataValidation=new IntFixedLengthValidator();
			break;

			case "IntRangeValidator":
				dataValidation=new IntRangeValidator();
			break;
			
			case "DoubleRangeValidator":
				dataValidation=new DoubleRangeValidator();
			break;

			case "StringRangeValidator":
				dataValidation=new StringRangeValidator();
			break;

			case "StringFixedLengthValidator":
				dataValidation=new StringFixedLengthValidator();
			break;

			case "BlackListValidator":
				dataValidation=new BlackListValidator();
			break;

			case "WhilteListValidator":
				dataValidation=new WhiteListValidator();
			break;
			
			case "NotNullValidator":
				dataValidation=new NotNullValidator();
			break;

			case "RegXValidator":
				dataValidation=new RegXValidator();
			break;

			case "PiiRule":
				dataValidation=new PiiRule();
			break;
			
			case "DateRangeValidator":
				dataValidation=new DateRangeValidator();
			break;
			default :
				dataValidation=new InvalidTypeValidator();
				

		}
		return dataValidation;
	}

}
