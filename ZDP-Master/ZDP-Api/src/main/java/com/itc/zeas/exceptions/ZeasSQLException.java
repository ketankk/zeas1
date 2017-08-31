package com.itc.zeas.exceptions;

public class ZeasSQLException extends ZeasException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */

	public ZeasSQLException() {

	}


	public ZeasSQLException(int errorCode, String errorMsg,String inputValue) {

		super(ZeasErrorCode.SQL_EXCEPTION, errorMsg, inputValue);

	}
	
	@Override
	public String toString() {
		return this.error+ " "+this.inputValue;
	}

}
