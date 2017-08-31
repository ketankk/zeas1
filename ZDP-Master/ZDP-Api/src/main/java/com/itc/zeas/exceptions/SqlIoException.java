package com.itc.zeas.exceptions;

public class SqlIoException{
	public static class IoException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public IoException(String message) {
			super(message);
		}
	}
	public static class SqlException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public SqlException(String message) {
			super(message);
		}
	}
}
