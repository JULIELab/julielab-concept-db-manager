package de.julielab.concepts.db.spi;

import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptCreator {
	Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
			throws ConceptCreationException, FacetCreationException;

	/**
	 * Checks if a concrete service provider implementation of this service provider
	 * interface matches the given name. The naming contract enforces that each
	 * provider must accept its own qualified class name as provider name. Other
	 * names are optional and might facilitate the usage of the provider in the
	 * configuration file.
	 * 
	 * @param providername
	 *            A name to check whether it matches the name of a provider.
	 * @return True, if the given name matches the provider's, false otherwise.
	 */
	boolean hasName(String providername);
}
