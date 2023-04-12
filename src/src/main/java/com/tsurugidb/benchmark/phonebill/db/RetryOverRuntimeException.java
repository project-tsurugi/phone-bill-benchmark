package com.tsurugidb.benchmark.phonebill.db;

public class RetryOverRuntimeException extends RuntimeException {

	public RetryOverRuntimeException() {
		super();
	}

	public RetryOverRuntimeException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RetryOverRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public RetryOverRuntimeException(String message) {
		super(message);
	}

	public RetryOverRuntimeException(Throwable cause) {
		super(cause);
	}

}
