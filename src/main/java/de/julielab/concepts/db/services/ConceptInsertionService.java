package de.julielab.concepts.db.services;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDBManagerException;
import de.julielab.concepts.util.ConceptDatabaseCreationException;
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

	public void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException, ConceptDatabaseCreationException {
		Iterator<ConceptInserter> inserterIt = loader.iterator();
		try {
			while (inserterIt.hasNext()) {
				ConceptInserter inserter = inserterIt.next();
				if (inserter.setConfiguration(connectionConfiguration)) {
					inserter.insertConcepts(concepts);
				}
			}
		} catch (URISyntaxException e) {
			throw new ConceptInsertionException(e);
		}
	}

	public void insertConcepts(Stream<ImportConcepts> concepts) {
		concepts.forEach(t -> {
			try {
				insertConcepts(t);
			} catch (ConceptDBManagerException e) {
				throw new UncheckedConceptDBManagerException(e);
			}
		});

	}
}
