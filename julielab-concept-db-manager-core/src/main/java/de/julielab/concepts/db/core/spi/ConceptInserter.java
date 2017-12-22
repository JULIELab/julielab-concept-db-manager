package de.julielab.concepts.db.core.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptInserter {
	void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfiguration, ImportConcepts concepts) throws ConceptInsertionException;

	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException;
}
