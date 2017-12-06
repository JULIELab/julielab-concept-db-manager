package de.julielab.concepts.db;

import org.neo4j.graphdb.GraphDatabaseService;

public interface FileDatabaseService {
	GraphDatabaseService getFileDatabase();
	void closeDatabase();
}
