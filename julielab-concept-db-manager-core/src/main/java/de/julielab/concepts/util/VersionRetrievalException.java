package de.julielab.concepts.util;

public class VersionRetrievalException extends VersioningException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 574904125132931082L;

	public VersionRetrievalException() {
		super();
	}

	public VersionRetrievalException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public VersionRetrievalException(String message, Throwable cause) {
		super(message, cause);
	}

	public VersionRetrievalException(String message) {
		super(message);
	}

	public VersionRetrievalException(Throwable cause) {
		super(cause);
	}

}
