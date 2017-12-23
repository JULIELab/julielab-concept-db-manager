package de.julielab.concepts.util;

public class VersioningException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6859837544636399231L;

	public VersioningException() {
		super();
	}

	public VersioningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public VersioningException(String message, Throwable cause) {
		super(message, cause);
	}

	public VersioningException(String message) {
		super(message);
	}

	public VersioningException(Throwable cause) {
		super(cause);
	}

}
