package de.julielab.concepts.util;

public class MethodCallException extends ConceptDBManagerException {
    public MethodCallException() {
    }

    public MethodCallException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MethodCallException(String message, Throwable cause) {
        super(message, cause);
    }

    public MethodCallException(String message) {
        super(message);
    }

    public MethodCallException(Throwable cause) {
        super(cause);
    }
}
