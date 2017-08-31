package com.itc.zeas.validation.rule;

public  class InvalidTypeValidator  implements DataValidation{
	
	private String validatorType;
	private String datatype;
	private String columnName;
	private String errorMsg;

	@Override
	public int isValidate(ValidationAttribute vAttr, String inputValue) {
		
		validatorType=vAttr.getValidatorType();
		datatype=vAttr.getDatatype();
		columnName=vAttr.getColumnName();
		return 1;
	}

	@Override
	public String performMasking(ValidationAttribute VAttr, String inputValue) {
		// TODO Auto-generated method stub
		return "";
	}

	@Override
	public String getError() {
		
		return validatorType+":"+datatype+":"+columnName;
	}

}
