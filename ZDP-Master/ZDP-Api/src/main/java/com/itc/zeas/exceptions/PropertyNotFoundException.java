package com.itc.zeas.exceptions;


public class PropertyNotFoundException extends ZeasException {

	private static final long serialVersionUID = 1L;
	protected String error;
	protected int errorCode;
	protected String inputValue;

	public PropertyNotFoundException() {
	}

	public PropertyNotFoundException(int errorCode, String errorMsg, String inputVlaue) {

		this.errorCode = errorCode;
		this.error = errorMsg;
		this.inputValue = inputVlaue;

	}

    public PropertyNotFoundException(String s) {
		this.error=s;
    }

    @Override
	public String toString() {
		return this.error + " " + this.inputValue;
	}

	public String getError() {
		return error;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public String getInputValue() {
		return inputValue;
	}
}
