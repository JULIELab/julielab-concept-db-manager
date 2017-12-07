package de.julielab.concepts.util;

public class ConceptDatabaseCreationException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4217836390810819769L;

	public ConceptDatabaseCreationException() {
		super();
	}

	public ConceptDatabaseCreationException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptDatabaseCreationException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptDatabaseCreationException(String message) {
		super(message);
	}

	public ConceptDatabaseCreationException(Throwable cause) {
		super(cause);
	}

}
