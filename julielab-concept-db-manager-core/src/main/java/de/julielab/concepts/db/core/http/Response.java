package de.julielab.concepts.db.core.http;

import java.util.List;

public class Response {
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

	public List<Result> getResults() {
		return results;
	}

	public void setResults(List<Result> results) {
		this.results = results;
	}

	public Result getSingleResult() {
		if (results.isEmpty())
			throw new IllegalStateException("There are no results.");
		if (results.size() > 1)
			throw new IllegalStateException("There is more than a single result.");
		return results.get(0);
	}
}
