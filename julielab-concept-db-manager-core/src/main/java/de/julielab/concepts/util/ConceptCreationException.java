package de.julielab.concepts.util;

public class ConceptCreationException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 857739344700278933L;

	public ConceptCreationException() {
		super();

	}

	public ConceptCreationException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);

	}

	public ConceptCreationException(String message, Throwable cause) {
		super(message, cause);

	}

	public ConceptCreationException(String message) {
		super(message);

	}

	public ConceptCreationException(Throwable cause) {
		super(cause);

	}

}
