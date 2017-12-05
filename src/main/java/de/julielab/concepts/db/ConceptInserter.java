package de.julielab.concepts.db;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptInserter {
	void insertConcepts(ImportConcepts concepts, HierarchicalConfiguration<ImmutableNode> insertConfiguration);
}
