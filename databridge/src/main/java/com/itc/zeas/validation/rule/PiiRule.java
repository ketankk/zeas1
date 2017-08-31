package com.itc.zeas.validation.rule;

import com.itc.zeas.validation.mask.PiiProtectorUtility;


public class PiiRule implements DataValidation {

	@Override
	/**
	 * Returning a static value 1 signifies we are always saying PIIRule doesn't 
	 * actually do any data validation, but just always says validation passed.
	 */
	public int isValidate(ValidationAttribute VAttr, String inputValue) {
		return 1;
	}

	@Override
	public String performMasking(ValidationAttribute VAttr, String inputValue) {
		
		String maskedValue=PiiProtectorUtility.performProtection(inputValue, VAttr.getValidationValue());
		return maskedValue;
	}

	@Override
	public String getError() {
		return "";
	}

}
