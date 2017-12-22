package de.julielab.concepts.db.core;

import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_ENDPOINT;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_NAME;
import static de.julielab.concepts.util.ConfigurationHelper.checkParameters;

import java.io.UnsupportedEncodingException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class ServerPluginConceptInserter implements ConceptInserter {

	private static final Logger log = LoggerFactory.getLogger(ServerPluginConceptInserter.class);
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	@Override
	public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfig, ImportConcepts concepts)
			throws ConceptInsertionException {
		try {
			checkParameters(importConfig, CONFKEY_PLUGIN_NAME, CONFKEY_PLUGIN_ENDPOINT);

			String pluginName = importConfig.getString(CONFKEY_PLUGIN_NAME);
			String pluginEndpoint = importConfig.getString(CONFKEY_PLUGIN_ENDPOINT);
			HttpConnectionService httpService = HttpConnectionService.getInstance();
			Gson gson = new Gson();
			HttpPost httpPost = httpService.getHttpPostRequest(connectionConfiguration,
					String.format(ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint));
			httpPost.setEntity(new StringEntity(gson.toJson(concepts)));

			String response = HttpConnectionService.getInstance().sendRequest(httpPost);
			log.debug("Server plugin response to concept insertion: {}", response);
		} catch (ConceptDatabaseConnectionException | UnsupportedEncodingException | ConceptCreationException e) {
			throw new ConceptInsertionException(e);
		}
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		try {
			HttpConnectionService httpService = HttpConnectionService.getInstance();
			// Check if there will be an error thrown due to an invalid URI or something.
			httpService.getHttpPostRequest(connectionConfiguration);
			this.connectionConfiguration = connectionConfiguration;
		} catch (ConceptDatabaseConnectionException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

}
