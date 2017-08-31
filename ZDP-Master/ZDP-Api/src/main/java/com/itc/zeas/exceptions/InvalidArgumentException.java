package com.itc.zeas.exceptions;

public class InvalidArgumentException {
	public static class InvalidPermissionLevelException extends
			RuntimeException {
		private static final long serialVersionUID = 1L;

		public InvalidPermissionLevelException(String message) {
			super(message);
		}
	}

	public static class InvalidResTypeException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public InvalidResTypeException(String message) {
			super(message);
		}
	}
}
