package de.julielab.concepts.db.core.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;

public interface FacetCreator extends ExtensionPoint {
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

}
