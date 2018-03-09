package de.julielab.concepts.util;

import java.util.stream.Collectors;

public class InternalNeo4jException extends ConceptDatabaseConnectionException {

    private Neo4jServerErrorResponse errorResponse;

    public InternalNeo4jException(Neo4jServerErrorResponse errorResponse) {
        super("Errors: " + errorResponse.getErrors().stream().map(Neo4jServerError::getMessage).collect(Collectors.joining("\n")));
        this.errorResponse = errorResponse;
    }

    public Neo4jServerErrorResponse getErrorResponse() {
        return errorResponse;
    }

    public void setErrorResponse(Neo4jServerErrorResponse errorResponse) {
        this.errorResponse = errorResponse;
    }
}
