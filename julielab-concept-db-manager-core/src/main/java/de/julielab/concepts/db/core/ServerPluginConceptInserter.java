package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_ENDPOINT;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_NAME;
import static de.julielab.java.utilities.ConfigurationUtilities.checkParameters;

public class ServerPluginConceptInserter implements ConceptInserter {

	private static final Logger log = LoggerFactory.getLogger(ServerPluginConceptInserter.class);
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	@Override
	public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfig, ImportConcepts concepts)
			throws ConceptInsertionException {
		try {
			checkParameters(importConfig, CONFKEY_PLUGIN_NAME, CONFKEY_PLUGIN_ENDPOINT);

			ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
			jsonMapper.setSerializationInclusion(Include.NON_NULL);
			jsonMapper.setSerializationInclusion(Include.NON_EMPTY);

			String serverUri = connectionConfiguration.getString(URI);
			String pluginName = importConfig.getString(PLUGIN_NAME);
			String pluginEndpoint = importConfig.getString(PLUGIN_ENDPOINT);
			HttpConnectionService httpService = HttpConnectionService.getInstance();
			HttpPost httpPost = httpService.getHttpPostRequest(connectionConfiguration, serverUri + String
					.format(ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint));
			Map<String, String> dataMap = new HashMap<>();
			dataMap.put(ConceptManager.KEY_CONCEPTS, jsonMapper.writeValueAsString(concepts.getConcepts()));
			dataMap.put(ConceptManager.KEY_FACET, jsonMapper.writeValueAsString(concepts.getFacet()));
			if (concepts.getImportOptions() != null)
				dataMap.put(ConceptManager.KEY_IMPORT_OPTIONS,
						jsonMapper.writeValueAsString(concepts.getImportOptions()));
			httpPost.setEntity(new StringEntity(jsonMapper.writeValueAsString(dataMap)));

			String response = HttpConnectionService.getInstance().sendRequest(httpPost);
			log.debug("Server plugin response to concept insertion: {}", response);
		} catch (ConceptDatabaseConnectionException | JsonProcessingException | UnsupportedEncodingException
				e) {
			throw new ConceptInsertionException(e);
		} catch (ConfigurationException e) {
			log.error("Configuration error occured with configuration {} {}", ConfigurationUtilities.LS, ConfigurationUtils.toString(importConfig));
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
