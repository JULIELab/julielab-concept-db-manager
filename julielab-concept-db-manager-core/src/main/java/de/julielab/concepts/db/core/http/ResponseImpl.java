package de.julielab.concepts.db.core.http;

import java.util.List;
import java.util.stream.Stream;

public class ResponseImpl implements Response {
	private List<Result> results;
	private List<Object> errors;
	public List<Object> getErrors() {
		return errors;
	}

	public void setErrors(List<Object> errors) {
		this.errors = errors;
	}

	@Override
	public String toString() {
		return "Response [results=" + results + ", errors=" + errors + "]";
	}

	public List<Result> getResultList() {
		return results;
	}

	public void setResults(List<Result> results) {
		this.results = results;
	}

	@Override
	public Stream<Result> getResults() {
		return results != null ? results.stream() : Stream.empty();
	}

	public Result getSingleResult() {
		if (results.isEmpty())
			throw new IllegalStateException("There are no results.");
		if (results.size() > 1)
			throw new IllegalStateException("There is more than a single result.");
		return results.get(0);
	}

	@Override
	public void close() throws Exception {
		// nothing to do
	}
}
