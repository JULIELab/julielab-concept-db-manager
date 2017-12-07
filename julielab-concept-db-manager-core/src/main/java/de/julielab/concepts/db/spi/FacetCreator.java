package de.julielab.concepts.db.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;

public interface FacetCreator {
	/**
	 * 
	 * @param facetConfiguration
	 *            The facet subconfiguration.
	 * @param facetData
	 *            An arbitrary object that might be required to gather all
	 *            information necessary to create a facet.
	 * @return An ImportFacet.
	 * @throws FacetCreationException
	 *             If facet creation fails.
	 */
	ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData)
			throws FacetCreationException;

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
