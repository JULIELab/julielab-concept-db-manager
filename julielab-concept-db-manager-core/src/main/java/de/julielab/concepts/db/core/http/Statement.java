package de.julielab.concepts.db.core.http;

import java.util.HashMap;
import java.util.Map;

public class Statement {
	private String statement;
	private Map<String, Object> parameters;

	public Statement(String statement, Object... parameters) {
		this.statement = statement;
		if (parameters.length != 0) {
			if (parameters.length % 2 != 0)
				throw new IllegalArgumentException(
						"The parameters must be of even size: The even indices are the parameter names, the odd indices are the values.");
			this.parameters = new HashMap<>();
			for (int i = 0; i < parameters.length; i++) {
				Object value = parameters[i];
				if (i % 2 == 1)
					this.parameters.put((String) parameters[i - 1], value);
			}
		}
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}

	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}
}