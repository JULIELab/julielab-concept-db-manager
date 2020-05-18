package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.java.utilities.ConfigurationUtilities.last;

public class ConceptCreationService implements ParameterExposing{

	private static final Logger log = LoggerFactory.getLogger(ConceptCreationService.class);

	private ServiceLoader<ConceptCreator> loader;
	private static ConceptCreationService service;

	private ConceptCreationService() {
		loader = ServiceLoader.load(ConceptCreator.class);
	}

	public static ConceptCreationService getInstance() {
		if (service == null)
			service = new ConceptCreationService();
		return service;
	}

	/**
	 * Expects an &lt;import&gt; configuration element and will return the
	 * corresponding concepts for database import.
	 * 
	 * @param importConfig
	 *            A subconfiguration on the &lt;import&gt; level.
	 * @return Concepts for database creation.
	 * @throws ConceptCreationException
	 *             If concept creation fails for any reason.
	 * @throws FacetCreationException
	 */
	public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
			throws ConceptCreationException, FacetCreationException {
		String providername = importConfig.getString(slash(CONCEPTS, CREATOR, NAME));
		Iterator<ConceptCreator> providerIt = loader.iterator();
		while (providerIt.hasNext()) {
			ConceptCreator conceptCreator = providerIt.next();
			if (conceptCreator.hasName(providername)) {
				log.debug("Invoking concept creator {}", providername);
				Stream<ImportConcepts> concepts = conceptCreator.createConcepts(importConfig);
				if (concepts == null)
					throw new ConceptCreationException("Concept creator " + providername + " did return null.");
				return concepts;
			}
		}
		throw new ConceptCreationException("No concept creator was found for the name " + providername
				+ ". Make sure that the desired concept creation provider is present in the META-INF/services/"
				+ ConceptCreator.class.getCanonicalName() + " file.");
	}

	@Override
	public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
		Iterator<ConceptCreator> providerIt = loader.iterator();
		while (providerIt.hasNext()) {
            template.addProperty(basePath, "");
			ConceptCreator conceptCreator = providerIt.next();
			conceptCreator.exposeParameters(last(basePath), template);
		}
	}
}
