package de.julielab.concepts.util;

public class DatabaseOperationException extends ConceptDBManagerException {
    public DatabaseOperationException() {
    }

    public DatabaseOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DatabaseOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseOperationException(String message) {
        super(message);
    }

    public DatabaseOperationException(Throwable cause) {
        super(cause);
    }
}
