package de.julielab.concepts.db.core.services;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDBManagerException;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.concepts.util.UncheckedConceptDBManagerException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class ConceptInsertionService {
	public static final String CONFKEY_PROVIDERNAME = "providername";
	public static final String CONFKEY_CONFIGURATION = "configuration";

	private ServiceLoader<ConceptInserter> loader;
	private static ConceptInsertionService service;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	private ConceptInsertionService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		loader = ServiceLoader.load(ConceptInserter.class);
	}

	public static ConceptInsertionService getInstance(
			HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		if (service == null)
			service = new ConceptInsertionService(connectionConfiguration);
		return service;
	}

	public void insertConcepts(ImportConcepts concepts)
			throws ConceptInsertionException, ConceptDatabaseConnectionException {
		Iterator<ConceptInserter> inserterIt = loader.iterator();
		boolean inserterFound = false;
		while (inserterIt.hasNext()) {
			ConceptInserter inserter = inserterIt.next();
			if (inserter.setConnection(connectionConfiguration)) {
				inserter.insertConcepts(concepts);
				inserterFound = true;
			}
		}
		if (!inserterFound)
			throw new ConceptInsertionException(
					"Concept insertion failed because no concept inserter for the connection configuration "
							+ ConfigurationUtils.toString(connectionConfiguration)
							+ " was found. Make sure that an appropriate connection provider is given in the META-INF/services/"
							+ ConceptInserter.class.getCanonicalName() + " file.");
	}

	public void insertConcepts(Stream<ImportConcepts> concepts) throws ConceptInsertionException {
		if (concepts == null)
			throw new ConceptInsertionException("The passed concepts object is null.");
		concepts.forEach(t -> {
			try {
				insertConcepts(t);
			} catch (ConceptDBManagerException e) {
				throw new UncheckedConceptDBManagerException(e);
			}
		});

	}
}
