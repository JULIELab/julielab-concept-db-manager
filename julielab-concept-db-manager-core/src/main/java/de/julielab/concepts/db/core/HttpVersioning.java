package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.concepts.db.core.http.*;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;

import static de.julielab.concepts.db.core.ConfigurationConstants.VERSION;
import static de.julielab.concepts.db.core.VersioningConstants.CREATE_VERSION;

public class HttpVersioning implements Versioning {

	private static final Logger log = LoggerFactory.getLogger(HttpVersioning.class);
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private HttpConnectionService httpService;

	@Override
	public void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException {
		String version = versioningConfig.getString(VERSION);
		String existingVersion = getVersion();
		if (null != existingVersion)
			throw new VersioningException("The database already has a version: " + existingVersion);
		Statements statements = new Statements(
				new Statement(CREATE_VERSION, VERSION, version));
		String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
		String transactionalUri = baseUri + Constants.TRANSACTION_ENDPOINT;
		try (Response response = httpService.sendStatements(statements, transactionalUri, connectionConfiguration)){
			if (!response.getErrors().isEmpty())
				throw new VersioningException(
						"Error happened when trying to create a version node: " + response.getErrors());
			log.info("Created database version node for version {}", version);
			statements = new Statements(
					new Statement(VersioningConstants.CREATE_UNIQUE_CONSTRAINT));
			try (Response response2 = httpService.sendStatements(statements, transactionalUri, connectionConfiguration)) {
				if (!response2.getErrors().isEmpty())
					throw new VersioningException(
							"Error happened when trying to create the unique constraint on VERSION.version: "
									+ response.getErrors());
			}
			log.info("Created UNIQUE constraint on the version node.");
		} catch (Exception e) {
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
			Response response = httpService.sendStatements(statements, baseUri + Constants.TRANSACTION_ENDPOINT,
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
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
		httpService = HttpConnectionService.getInstance();
		// Check if there will be an error thrown due to an invalid URI or something.
		httpService.getHttpRequest(connectionConfiguration, HttpMethod.GET);
		this.connectionConfiguration = connectionConfiguration;
	}
}
