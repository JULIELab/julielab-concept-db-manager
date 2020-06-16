package de.julielab.concepts.db.core.http;

import java.util.List;
import java.util.stream.Stream;

public interface Response extends AutoCloseable {
    Stream<Result> getResults();

    Result getSingleResult();

    List<Object> getErrors();
}
