package com.itc.zeas.exceptions;

public class ZeasFileNotFoundException extends ZeasException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ZeasFileNotFoundException() {

	}

	public ZeasFileNotFoundException(int errorCode, String errorMsg,String inputVlaue) {

		super(ZeasErrorCode.FILE_NOT_FOUND, errorMsg, inputVlaue);

	}
	
	@Override
	public String toString() {
		return error+ " " +inputValue;
	}

}
