package de.julielab.concepts.db.core;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;

public class ServerPluginExporter extends DataExporterBase {

	private static final String CONFKEY_PLUGIN_ENDPOINT = "configuration.pluginendpoint";
	private static final String CONFKEY_PLUGIN_NAME = "configuration.pluginname";
	private static final String ADDRESS_FMT = "/db/data/ext/%s/graphdb/%s";
	
	
	private static final Logger log = LoggerFactory.getLogger(ServerPluginExporter.class);

	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	@Override
	public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
			throws ConceptDatabaseConnectionException, DataExportException {
		String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
		String pluginName = exportConfig.getString(CONFKEY_PLUGIN_NAME);
		String pluginEndpoint = exportConfig.getString(CONFKEY_PLUGIN_ENDPOINT);
		String outputFile = exportConfig.getString(CONFKEY_OUTPUT_FILE);
		HierarchicalConfiguration<ImmutableNode> parameterConfiguration = exportConfig
				.configurationAt(CONFKEY_PARAMETERS);
		Map<String, Parameter> parameterMap = parseParameters(parameterConfiguration);
		Map<String, Object> parameters = parameterMap.values().stream()
				.collect(Collectors.toMap(Parameter::getName, Parameter::getValue));

		HttpConnectionService httpService = HttpConnectionService.getInstance();
		String completePluginEndpointUri = baseUri + String.format(ADDRESS_FMT, pluginName, pluginEndpoint);
		HttpPost request = httpService.getHttpPostRequest(parameterConfiguration, completePluginEndpointUri);
		Gson gson = new Gson();
		try {
			request.setEntity(new StringEntity(gson.toJson(parameters)));
			log.info("Sending request {} to {}", parameters, completePluginEndpointUri);
			String response = httpService.sendRequest(request);
			log.info("Writing file {}", outputFile);
			writeBase64GzipToFile(outputFile, response);
			log.info("Done.");
		} catch (UnsupportedEncodingException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

	@Override
	public boolean hasName(String providerName) {
		return providerName.equalsIgnoreCase("httpserverplugins") || providerName.equals(getClass().getCanonicalName());
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
