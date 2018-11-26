package de.julielab.concepts.util;

import java.util.List;

public class Neo4jServerErrorResponse {
    private String message;
    private String exception;
    private String fullname;

    private List<String> stackTrace;
    private List<Neo4jServerError> errors;

    private Neo4jServerErrorResponse cause;

    public Neo4jServerErrorResponse getCause() {
        return cause;
    }

    public void setCause(Neo4jServerErrorResponse cause) {
        this.cause = cause;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public List<String> getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(List<String> stackTrace) {
        this.stackTrace = stackTrace;
    }

    public List<Neo4jServerError> getErrors() {
        return errors;
    }

    public void setErrors(List<Neo4jServerError> errors) {
        this.errors = errors;
    }
}
