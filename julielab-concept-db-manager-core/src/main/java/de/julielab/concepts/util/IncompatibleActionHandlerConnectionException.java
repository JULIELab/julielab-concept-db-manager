package de.julielab.concepts.util;

public class IncompatibleActionHandlerConnectionException extends ConceptDBManagerException {


    public IncompatibleActionHandlerConnectionException() {
        super();
    }

    public IncompatibleActionHandlerConnectionException(String message, Throwable cause, boolean enableSuppression,
                                                        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public IncompatibleActionHandlerConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public IncompatibleActionHandlerConnectionException(String message) {
        super(message);
    }

    public IncompatibleActionHandlerConnectionException(Throwable cause) {
        super(cause);
    }

}
