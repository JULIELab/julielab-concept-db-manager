package de.julielab.concepts.util;

public class MappingInsertionException extends ConceptDBManagerException {
    public MappingInsertionException() {
    }

    public MappingInsertionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MappingInsertionException(String message, Throwable cause) {
        super(message, cause);
    }

    public MappingInsertionException(String message) {
        super(message);
    }

    public MappingInsertionException(Throwable cause) {
        super(cause);
    }
}
