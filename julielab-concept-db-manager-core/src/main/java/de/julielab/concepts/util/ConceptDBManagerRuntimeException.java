package de.julielab.concepts.util;

public class ConceptDBManagerRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5751235493428899198L;

	public ConceptDBManagerRuntimeException() {
		super();
	}

	public ConceptDBManagerRuntimeException(String message, Throwable cause, boolean enableSuppression,
                                            boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptDBManagerRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptDBManagerRuntimeException(String message) {
		super(message);
	}

	public ConceptDBManagerRuntimeException(Throwable cause) {
		super(cause);
	}

}
