package de.julielab.concepts.db.services;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.application.ConceptDatabaseApplication;
import de.julielab.concepts.util.ConceptDatabaseCreationException;

public class FileDatabaseService {

	private static final Logger log = LoggerFactory.getLogger(FileDatabaseService.class);

	public static final String CONFKEY_URI = "uri";

	private Map<String, GraphDatabaseService> dbs;

	private static FileDatabaseService service;
	private GraphDatabaseFactory graphDatabaseFactory;

	private FileDatabaseService() {
		graphDatabaseFactory = new GraphDatabaseFactory();
		dbs = new HashMap<>();
	}

	public static FileDatabaseService getInstance() {
		if (service == null)
			service = new FileDatabaseService();
		return service;
	}

	public GraphDatabaseService getDatabase(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws URISyntaxException, ConceptDatabaseCreationException {
		String uriString;
		if ((uriString = connectionConfiguration.getString(CONFKEY_URI)) == null)
			throw new IllegalArgumentException("The passed configuration does not have the property \"" + CONFKEY_URI
					+ "\". Make sure to only pass the \"" + ConceptDatabaseApplication.CONFKEY_CONNECTION
					+ "\" subconfiguration.");
		URI dbUri = new URI(uriString);
		String scheme = dbUri.getScheme();
		if ((scheme != null) && !scheme.equalsIgnoreCase("file"))
			return null;
		File dbFile = dbUri.isAbsolute() ? new File(dbUri) : new File(dbUri.getRawSchemeSpecificPart());
		log.info("Accessing file database located at {}", dbFile);
		try {
			return dbs.computeIfAbsent(dbFile.getCanonicalPath(),
					k -> graphDatabaseFactory.newEmbeddedDatabase(dbFile));
		} catch (IOException e) {
			throw new ConceptDatabaseCreationException(e);
		}
	}

	/**
	 * Shuts down all graph databases opened by this service.
	 */
	public void shutdown() {
		dbs.values().forEach(GraphDatabaseService::shutdown);
		dbs.clear();
	}

}
