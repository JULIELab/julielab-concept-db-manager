package de.julielab.concepts.db.core.http;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamingResponse implements Response {
    private InputStream responseStream;

    private List<Object> errors = Collections.emptyList();

    private boolean isConsumed = false;

    public StreamingResponse(InputStream responseStream) {

        this.responseStream = responseStream;
    }

    public List<Object> getErrors() {
        return errors;
    }

    @Override
    public Stream<Result> getResults() {
        if (isConsumed)
            throw new IllegalStateException("This streaming result was already consumed.");
        //{"results":[{"columns":["version"],"data":[]}],"errors":[]}
        ObjectMapper om = new ObjectMapper();
        try {
            JsonParser parser = new JsonFactory(om).createParser(responseStream);
            Iterator<Result> it = new Iterator<>() {
                String currentField = null;
                Iterator<Result> it;

                @Override
                public boolean hasNext() {
                    boolean hasNext = false;
                    try {
                        while ((it == null || !it.hasNext()) && parser.nextToken() != null) {
                            JsonToken currentToken = parser.getCurrentToken();
                            if (currentToken == JsonToken.FIELD_NAME) {
                                currentField = parser.getCurrentName();
                            } else if (currentToken == JsonToken.START_ARRAY && currentField.equals("results")) {
                                parser.nextToken();
                                it = parser.readValuesAs(Result.class);
                            } else if (currentToken == JsonToken.START_ARRAY && currentField.equals("errors")) {
                                Iterator<Object> errorIt = parser.readValuesAs(Object.class);
                                errors = new ArrayList<>();
                                while (errorIt.hasNext()) {
                                    Object error = errorIt.next();
                                    errors.add(error);
                                }
                            }
                        }
                        hasNext = it != null ? it.hasNext() && parser.currentToken() != JsonToken.END_ARRAY: false;
                        if (!hasNext) {
                            parser.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return hasNext;
                }

                @Override
                public Result next() {
                    hasNext();
                    return it != null ? it.next() : null;
                }
            };
             isConsumed = true;
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.IMMUTABLE), false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Stream.empty();
    }

    @Override
    public Result getSingleResult() {
        Iterator<Result> it = getResults().iterator();
        if (!it.hasNext())
            throw new IllegalStateException("There are no results.");
        Result result = it.next();
        if (it.hasNext())
            throw new IllegalStateException("There is more than a single result.");
        return result;
    }

    @Override
    public void close() throws Exception {
        try {
            if (responseStream != null)
                responseStream.close();
        } catch (IOException e) {
            // nothing
        }
    }
}
