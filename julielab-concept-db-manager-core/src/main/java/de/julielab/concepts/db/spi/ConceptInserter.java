package de.julielab.concepts.db.spi;

import java.net.URISyntaxException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseCreationException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptInserter {
	void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException;

	boolean setConfiguration(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws URISyntaxException, ConceptDatabaseCreationException;
}
