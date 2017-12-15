package de.julielab.concepts.util;

public class ConceptDatabaseConnectionException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4217836390810819769L;

	public ConceptDatabaseConnectionException() {
		super();
	}

	public ConceptDatabaseConnectionException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptDatabaseConnectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptDatabaseConnectionException(String message) {
		super(message);
	}

	public ConceptDatabaseConnectionException(Throwable cause) {
		super(cause);
	}

}
