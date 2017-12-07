package de.julielab.concepts.util;

public class FacetCreationException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7378653918087791825L;

	public FacetCreationException() {
		super();
	}

	public FacetCreationException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public FacetCreationException(String message, Throwable cause) {
		super(message, cause);
	}

	public FacetCreationException(String message) {
		super(message);
	}

	public FacetCreationException(Throwable cause) {
		super(cause);
	}

}
