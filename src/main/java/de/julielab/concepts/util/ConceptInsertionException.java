package de.julielab.concepts.util;

public class ConceptInsertionException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8518033701377206636L;

	public ConceptInsertionException() {
		super();
	}

	public ConceptInsertionException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptInsertionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptInsertionException(String message) {
		super(message);
	}

	public ConceptInsertionException(Throwable cause) {
		super(cause);
	}

}
