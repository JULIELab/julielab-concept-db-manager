package de.julielab.concepts.db;

import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptCreator {
	Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> config) throws ConceptCreationException;
}
