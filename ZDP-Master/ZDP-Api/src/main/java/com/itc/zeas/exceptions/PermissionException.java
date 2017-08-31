package com.itc.zeas.exceptions;

public class PermissionException {
	public static class NotACreatorException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public NotACreatorException(String message) {
			super(message);
		}
	}

	public static class NotASuperUserException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public NotASuperUserException(String message) {
			super(message);
		}
	}

	public static class NotHaveRequestedPermissionException extends
			RuntimeException {
		private static final long serialVersionUID = 1L;

		public NotHaveRequestedPermissionException(String message) {
			super(message);
		}
	}
}