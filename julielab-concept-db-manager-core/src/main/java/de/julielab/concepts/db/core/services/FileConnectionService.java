package de.julielab.concepts.db.core.services;

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

import de.julielab.concepts.util.ConceptDatabaseConnectionException;

public class FileConnectionService {

	private static final Logger log = LoggerFactory.getLogger(FileConnectionService.class);

	public static final String CONFKEY_URI = "uri";
	public static final String CONFKEY_CONNECTION = "connection";

	private Map<String, GraphDatabaseService> dbs;

	private static FileConnectionService service;
	private GraphDatabaseFactory graphDatabaseFactory;

	private FileConnectionService() {
		graphDatabaseFactory = new GraphDatabaseFactory();
		dbs = new HashMap<>();
	}

	public static FileConnectionService getInstance() {
		if (service == null)
			service = new FileConnectionService();
		return service;
	}

	public GraphDatabaseService getDatabase(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		String uriString;
		if ((uriString = connectionConfiguration.getString(CONFKEY_URI)) == null)
			throw new ConceptDatabaseConnectionException("The passed configuration does not have the property \""
					+ CONFKEY_URI + "\". Make sure to only pass the \"" + CONFKEY_CONNECTION + "\" subconfiguration.");
		try {
			URI dbUri = new URI(uriString);
			String scheme = dbUri.getScheme();
			if ((scheme != null) && !scheme.equalsIgnoreCase("file"))
				throw new ConceptDatabaseConnectionException("The given URI \"" + dbUri
						+ " is neither a relative file path without scheme nor an absolute file path with file: scheme.");
			File dbFile = dbUri.isAbsolute() ? new File(dbUri) : new File(dbUri.getRawSchemeSpecificPart());
			log.debug("Accessing file database located at {}", dbFile);
			return dbs.computeIfAbsent(dbFile.getCanonicalPath(),
					k -> graphDatabaseFactory.newEmbeddedDatabase(dbFile));
		} catch (IOException | URISyntaxException e) {
			throw new ConceptDatabaseConnectionException(e);
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
