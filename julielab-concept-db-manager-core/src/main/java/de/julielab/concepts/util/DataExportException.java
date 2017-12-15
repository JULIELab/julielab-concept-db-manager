package de.julielab.concepts.util;

public class DataExportException extends ConceptDBManagerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1499859308482724145L;

	public DataExportException() {
		super();
	}

	public DataExportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DataExportException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataExportException(String message) {
		super(message);
	}

	public DataExportException(Throwable cause) {
		super(cause);
	}

}
