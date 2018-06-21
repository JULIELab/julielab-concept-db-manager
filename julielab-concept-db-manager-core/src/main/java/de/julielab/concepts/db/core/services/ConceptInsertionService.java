package de.julielab.concepts.db.core.services;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDBManagerException;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.concepts.util.ConceptDBManagerRuntimeException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class ConceptInsertionService {
	
	private static final Logger log = LoggerFactory.getLogger(ConceptInsertionService.class);
	
	public static final String CONFKEY_PROVIDERNAME = "providername";

	private ServiceLoader<ConceptInserter> loader;

	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private static ConceptInsertionService service;
	private static Map<HierarchicalConfiguration<ImmutableNode>, ConceptInsertionService> serviceMap;

	private ConceptInsertionService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		loader = ServiceLoader.load(ConceptInserter.class);
	}

	/**
	 * Returns the data export service singleton specifically created for this
	 * passed configuration.
	 * 
	 * @param connectionConfiguration
	 * @return
	 */
	public static synchronized ConceptInsertionService getInstance(
			HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		if (serviceMap == null)
			serviceMap = new HashMap<>();
		return serviceMap.computeIfAbsent(connectionConfiguration, ConceptInsertionService::new);
	}

	public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfiguration, ImportConcepts concepts)
			throws ConceptInsertionException {
		Iterator<ConceptInserter> inserterIt = loader.iterator();
		boolean inserterFound = false;
		while (inserterIt.hasNext()) {
			ConceptInserter inserter = inserterIt.next();
			try {
				inserter.setConnection(connectionConfiguration); 
				inserter.insertConcepts(importConfiguration, concepts);
				inserterFound = true;
			} catch (ConceptDatabaseConnectionException e) {
				log.debug("Concept inserter " + inserter.getClass().getCanonicalName() + " could not serve the connection configuration " + ConfigurationUtils.toString(connectionConfiguration) + ": " + e.getMessage());
			}
		}
		if (!inserterFound)
			throw new ConceptInsertionException(
					"Concept insertion failed because no concept inserter for the connection configuration "
							+ ConfigurationUtils.toString(connectionConfiguration)
							+ " was found. Make sure that an appropriate connection provider is given in the META-INF/services/"
							+ ConceptInserter.class.getCanonicalName() + " file.");
	}

	public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfiguration, Stream<ImportConcepts> concepts) throws ConceptInsertionException {
		if (concepts == null)
			throw new ConceptInsertionException("The passed concepts object is null.");
		concepts.forEach(t -> {
			try {
				insertConcepts(importConfiguration, t);
			} catch (ConceptDBManagerException e) {
				throw new ConceptDBManagerRuntimeException(e);
			}
		});

	}
}
