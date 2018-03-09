package de.julielab.concepts.db.core;

import static de.julielab.concepts.db.core.VersioningConstants.CREATE_VERSION;
import static de.julielab.concepts.db.core.VersioningConstants.PROP_VERSION;

import java.io.IOException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.julielab.concepts.db.core.http.Response;
import de.julielab.concepts.db.core.http.Result;
import de.julielab.concepts.db.core.http.Statement;
import de.julielab.concepts.db.core.http.Statements;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;

public class HttpVersioning implements Versioning {

	private static final Logger log = LoggerFactory.getLogger(HttpVersioning.class);
	private static final String TRANSACTION_ENDPOINT = "/db/data/transaction/commit";
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private HttpConnectionService httpService;

	@Override
	public void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException {
		String version = versioningConfig.getString(VersioningConstants.CONFKEY_VERSION);
		String existingVersion = getVersion();
		if (null != existingVersion)
			throw new VersioningException("The database already has a version: " + existingVersion);
		Statements statements = new Statements(
				new Statement(CREATE_VERSION, PROP_VERSION, version));
		String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
		String transactionalUri = baseUri + TRANSACTION_ENDPOINT;
		try {
			Response response = httpService.sendStatements(statements, transactionalUri, connectionConfiguration);
			if (!response.getErrors().isEmpty())
				throw new VersioningException(
						"Error happened when trying to create a version node: " + response.getErrors());
			log.info("Created database version node for version {}", version);
			statements = new Statements(
					new Statement(VersioningConstants.CREATE_UNIQUE_CONSTRAINT));
			response = httpService.sendStatements(statements, transactionalUri, connectionConfiguration);
			if (!response.getErrors().isEmpty())
				throw new VersioningException(
						"Error happened when trying to create the unique constraint on VERSION.version: "
								+ response.getErrors());
			log.info("Created UNIQUE constraint on the version node.");
		} catch (ConceptDatabaseConnectionException | IOException e) {
			throw new VersioningException(e);
		}
	}

	@Override
	public String getVersion() throws VersionRetrievalException {
		Statements statements = new Statements(new Statement(VersioningConstants.GET_VERSION));
		ObjectMapper jsonMapper = new ObjectMapper();
		jsonMapper.setSerializationInclusion(Include.NON_NULL);
		jsonMapper.setSerializationInclusion(Include.NON_EMPTY);
		try {
			String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
			Response response = httpService.sendStatements(statements, baseUri + TRANSACTION_ENDPOINT,
					connectionConfiguration);
			Result result = response.getSingleResult();
			if (!result.getData().isEmpty())
				return (String) result.getData(0).getRow(0);
		} catch (ConceptDatabaseConnectionException | IOException e) {
			throw new VersionRetrievalException(e);
		}
		return null;
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		try {
			httpService = HttpConnectionService.getInstance();
			// Check if there will be an error thrown due to an invalid URI or something.
			httpService.getHttpPostRequest(connectionConfiguration);
			this.connectionConfiguration = connectionConfiguration;
		} catch (ConceptDatabaseConnectionException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}
}