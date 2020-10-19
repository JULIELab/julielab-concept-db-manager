package de.julielab.concepts.db.core.services;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.java.utilities.CLIInteractionUtilities;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class BoltConnectionService implements ParameterExposing {

	private final Map<NetworkConnectionCredentials, Driver> drivers;
	private static BoltConnectionService service;

	public BoltConnectionService() {
		this.drivers = new HashMap<>();
	}
	
	public static BoltConnectionService getInstance() {
		if (service == null)
			service = new BoltConnectionService();
		return service;
	}

	public synchronized Driver getBoltDriver(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws IOException, ConceptDatabaseConnectionException {
		String boltUri = connectionConfiguration.getString(URI);
		String user = connectionConfiguration.getString(USER);
		String password = connectionConfiguration.getString(PASSWORD);

		checkForBoltScheme(boltUri);
		
		if (user != null && password == null)
			password = aquirePasswordInteractively(user);
		NetworkConnectionCredentials key = new NetworkConnectionCredentials(boltUri, user, password);
		try {
			return drivers.computeIfAbsent(key,
					k -> GraphDatabase.driver(k.getUri(), k.isEmpty()? AuthTokens.none() : AuthTokens.basic(k.getUser(), k.getPassword())));
		} catch (Exception e) {
			throw new ConceptDatabaseConnectionException(
					"Failed to connect to Neo4j database via bolt with the configuration "
							+ ConfigurationUtils.toString(connectionConfiguration), e);
		}
	}

	private void checkForBoltScheme(String boltUri) throws ConceptDatabaseConnectionException {
		try {
			URI uri = new URI(boltUri);
			if (uri.getScheme() == null || !uri.getScheme().equalsIgnoreCase("bolt"))
				throw new ConceptDatabaseConnectionException(
						"The given URI " + boltUri + " does not specify the bolt scheme.");
		} catch (URISyntaxException e1) {
			throw new ConceptDatabaseConnectionException(e1);
		}
	}

	private String aquirePasswordInteractively(String user) throws IOException {
		return CLIInteractionUtilities.readLineFromStdInWithMessage(
				"Please specify the Neo4j database password for the user \"" + user + "\": ");
	}

	@Override
	public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
		template.setProperty(slash(basePath, URI), "");
		template.setProperty(slash(basePath, USER), "");
		template.setProperty(slash(basePath, PASSWORD), "");
	}
}
