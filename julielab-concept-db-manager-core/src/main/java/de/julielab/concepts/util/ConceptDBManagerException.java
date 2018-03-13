package de.julielab.concepts.util;

import de.julielab.jssf.commons.util.JSSFException;

public class ConceptDBManagerException extends JSSFException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6572268704972297647L;

	public ConceptDBManagerException() {
		super();
	}

	public ConceptDBManagerException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptDBManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptDBManagerException(String message) {
		super(message);
	}

	public ConceptDBManagerException(Throwable cause) {
		super(cause);
	}

}
