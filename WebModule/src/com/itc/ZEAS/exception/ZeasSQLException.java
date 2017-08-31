package com.itc.zeas.exception;

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

	public ZeasSQLException(int errorCode, String errorMsg,String inputVlaue) {

		super(ZeasErrorCode.SQL_EXCEPTION, errorMsg, inputVlaue);

	}
	
	@Override
	public String toString() {
		return this.error+ " "+this.inputValue;
	}

}
