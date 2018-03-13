package de.julielab.concepts.util;

public class MappingCreationException extends ConceptDBManagerException {

    private static final long serialVersionUID = 6053545750492774785L;

    public MappingCreationException() {
    }

    public MappingCreationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MappingCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MappingCreationException(String message) {
        super(message);
    }

    public MappingCreationException(Throwable cause) {
        super(cause);
    }
}
