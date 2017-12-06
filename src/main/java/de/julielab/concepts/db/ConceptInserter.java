package de.julielab.concepts.db;

import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptInserter {
	void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException;
}
