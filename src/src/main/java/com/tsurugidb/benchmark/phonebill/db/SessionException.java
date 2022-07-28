package com.tsurugidb.benchmark.phonebill.db;

public class SessionException extends Exception {

	public SessionException(String message, Throwable cause) {
		super(message, cause);
	}

	public SessionException(Throwable cause) {
		super(cause);
	}

}
