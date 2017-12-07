package de.julielab.concepts.util;

public class UncheckedConceptDBManagerException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5751235493428899198L;

	public UncheckedConceptDBManagerException() {
		super();
	}

	public UncheckedConceptDBManagerException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public UncheckedConceptDBManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public UncheckedConceptDBManagerException(String message) {
		super(message);
	}

	public UncheckedConceptDBManagerException(Throwable cause) {
		super(cause);
	}

}
