package de.julielab.concepts.db.core.services;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;

public class FacetCreationService {

	public static final String CONFKEY_FACET = "configuration.facet";
	public static final String CONFKEY_FACET_CREATOR = "facetcreator";

	private ServiceLoader<FacetCreator> serviceLoader;
	private static FacetCreationService service;

	private FacetCreationService() {
		serviceLoader = ServiceLoader.load(FacetCreator.class);
	}

	public static FacetCreationService getInstance() {
		if (service == null)
			service = new FacetCreationService();
		return service;
	}

	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> importConfiguration, Object facetData)
			throws FacetCreationException {
		String facetCreatorName = importConfiguration.getString(CONFKEY_FACET_CREATOR);
		Iterator<FacetCreator> creatorIt = serviceLoader.iterator();
		while (creatorIt.hasNext()) {
			FacetCreator facetCreator = creatorIt.next();
			if (facetCreator.hasName(facetCreatorName))
				return facetCreator.createFacet(importConfiguration.configurationAt(CONFKEY_FACET), facetData);
		}
		return null;
	}

	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> importConfiguration)
			throws FacetCreationException {
		return createFacet(importConfiguration, null);
	}
}
