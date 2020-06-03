package de.julielab.concepts.db.core.services;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class FileConnectionService {

	private static final Logger log = LoggerFactory.getLogger(FileConnectionService.class);

	public static final String CONFKEY_URI = "uri";
	public static final String CONFKEY_CONNECTION = "connection";

	private Map<String, DatabaseManagementService> dbs;

	private static FileConnectionService service;

	private FileConnectionService() {
		dbs = new HashMap<>();
	}

	public static FileConnectionService getInstance() {
		if (service == null)
			service = new FileConnectionService();
		return service;
	}

	public GraphDatabaseService getDefaultGraphDatabase(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
		return getDatabaseManagementService(connectionConfiguration).database(DEFAULT_DATABASE_NAME);
	}

	public DatabaseManagementService getDatabaseManagementService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
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
			log.debug("Accessing file database located at {} that currently {}", dbFile.getAbsolutePath(), dbFile.exists() ? "exists" : "does not exist");
			return dbs.computeIfAbsent(dbFile.getCanonicalPath(),
					k -> new DatabaseManagementServiceBuilder(dbFile).build());
		} catch (IOException | URISyntaxException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

	/**
	 * Shuts down all graph databases opened by this service.
	 */
	public void shutdown() {
		dbs.values().forEach(DatabaseManagementService::shutdown);
		dbs.clear();
	}

}
