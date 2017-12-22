package de.julielab.concepts.db.core.services;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;

public class FacetCreationService {

	private static final Logger log = LoggerFactory.getLogger(FacetCreationService.class);
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
		if (StringUtils.isBlank(facetCreatorName))
			log.debug("Facet creator name is not given, will fall back to the default facet.");
		Iterator<FacetCreator> creatorIt = serviceLoader.iterator();
		ImportFacet facet = null;
		boolean providerFound = false;
		while (creatorIt.hasNext()) {
			FacetCreator facetCreator = creatorIt.next();
			if (facetCreator.hasName(facetCreatorName)) {
				if (importConfiguration.containsKey(CONFKEY_FACET))
					facet = facetCreator.createFacet(importConfiguration.configurationAt(CONFKEY_FACET), facetData);
				else
					// There is no facet configuration, just pass the import configuration to avoid
					// NPEs in the default facet creator which will be used.
					facet = facetCreator.createFacet(importConfiguration, facetData);
				providerFound = true;
				break;
			}
		}
		if (!providerFound)
			throw new FacetCreationException(
					"No facet creation provider for the name \"" + facetCreatorName + "\" could be found.");
		if (facet == null)
			throw new FacetCreationException(
					"The facet creator \"" + facetCreatorName + "\" did not create a facet for the facet configuration "
							+ ConfigurationUtils.toString(importConfiguration.configurationAt(CONFKEY_FACET)));
		return facet;
	}

	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> importConfiguration)
			throws FacetCreationException {
		return createFacet(importConfiguration, null);
	}
}
