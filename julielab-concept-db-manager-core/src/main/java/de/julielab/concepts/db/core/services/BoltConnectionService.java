package de.julielab.concepts.db.core.services;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.java.utilities.CLIInteractionUtilities;

public class BoltConnectionService {

	public static final String CONFKEY_URI = "uri";
	public static final String CONFKEY_USER = "user";
	public static final String CONFKEY_PASSW = "password";

	private Map<Credentials, Driver> drivers;
	private static BoltConnectionService service;

	public static BoltConnectionService getInstance() {
		if (service == null)
			service = new BoltConnectionService();
		return service;
	}

	public synchronized Driver getBoltDriver(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws IOException, ConceptDatabaseConnectionException {
		String boltUri = connectionConfiguration.getString(CONFKEY_URI);
		String user = connectionConfiguration.getString(CONFKEY_USER);
		String password = connectionConfiguration.getString(CONFKEY_PASSW);
		if (password == null)
			password = aquirePasswordInteractively(user);
		Credentials key = new Credentials(boltUri, user, password);
		try {
			return drivers.computeIfAbsent(key,
					k -> GraphDatabase.driver(k.boltUri, AuthTokens.basic(k.user, k.password)));
		} catch (Exception e) {
			throw new ConceptDatabaseConnectionException("Failed to connect to Neo4j database via bolt with the configuration "
					+ ConfigurationUtils.toString(connectionConfiguration));
		}
	}

	private String aquirePasswordInteractively(String user) throws IOException {
		return CLIInteractionUtilities.readLineFromStdInWithMessage(
				"Please specify the Neo4j database password for the user \"" + user + "\": ");
	}

	private class Credentials {
		private String boltUri;
		private String user;
		private String password;

		public Credentials(String boltUri, String user, String password) {
			this.boltUri = boltUri;
			this.user = user;
			this.password = password;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((boltUri == null) ? 0 : boltUri.hashCode());
			result = prime * result + ((password == null) ? 0 : password.hashCode());
			result = prime * result + ((user == null) ? 0 : user.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Credentials other = (Credentials) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (boltUri == null) {
				if (other.boltUri != null)
					return false;
			} else if (!boltUri.equals(other.boltUri))
				return false;
			if (password == null) {
				if (other.password != null)
					return false;
			} else if (!password.equals(other.password))
				return false;
			if (user == null) {
				if (other.user != null)
					return false;
			} else if (!user.equals(other.user))
				return false;
			return true;
		}

		private BoltConnectionService getOuterType() {
			return BoltConnectionService.this;
		}

	}
}
