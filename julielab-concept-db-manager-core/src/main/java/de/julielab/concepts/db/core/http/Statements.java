package de.julielab.concepts.db.core.http;

import java.util.ArrayList;
import java.util.List;

public class Statements {
	private List<Statement> statements;

	public Statements() {
		this.statements = new ArrayList<>();
	}

	public Statements(Statement... statements) {
		this.statements = new ArrayList<>(statements.length);
		for (int i = 0; i < statements.length; i++) {
			Statement statement = statements[i];
			this.statements.add(statement);
		}
	}

	public List<Statement> getStatements() {
		return statements;
	}

	public void setStatements(List<Statement> statements) {
		this.statements = statements;
	}

	public void addStatement(Statement statement) {
		statements.add(statement);
	}
}