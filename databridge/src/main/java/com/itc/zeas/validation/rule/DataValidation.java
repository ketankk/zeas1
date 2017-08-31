package com.itc.zeas.validation.rule;

public interface DataValidation {

	int isValidate(ValidationAttribute VAttr, String inputValue);
	
	String performMasking(ValidationAttribute VAttr, String inputValue);
	
	String getError();

}
