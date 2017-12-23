package de.julielab.concepts.db.core;

import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_ENDPOINT;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_NAME;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.java.utilities.FileUtilities;

public class ServerPluginExporter extends DataExporterBase {

	private static final String CONFKEY_DECODING = "configuration.decoding";

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
				.collect(Collectors.toMap(Parameter::getName, Parameter::getRequestValue));

		HttpConnectionService httpService = HttpConnectionService.getInstance();
		String completePluginEndpointUri = baseUri + String.format(SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint);
		HttpPost request = httpService.getHttpPostRequest(connectionConfiguration, completePluginEndpointUri);
		Gson gson = new Gson();
		String response = null;
		try {
			String parameterJson = gson.toJson(parameters);
			request.setEntity(new StringEntity(parameterJson));
			log.info("Sending request {} to {}", parameterJson, completePluginEndpointUri);
			response = httpService.sendRequest(request);
			log.info("Writing file {}", outputFile);
			String decodedResponse = decode(response, exportConfig.configurationAt(CONFKEY_DECODING));
			try (BufferedWriter bw = FileUtilities.getWriterToFile(new File(outputFile))) {
				bw.write(decodedResponse);
			}
			log.info("Done.");
		} catch (UnsupportedEncodingException e) {
			throw new ConceptDatabaseConnectionException(e);
		} catch (ConceptDatabaseConnectionException e) {
			log.error("Connection error when posting parameters {} to plugin {}, endpoint {}", parameters, pluginName,
					pluginEndpoint);
			throw e;
		} catch (IOException e) {
			throw new DataExportException("Decoding the retrieved data failed. Decoding configuration is "
					+ ConfigurationUtils.toString(exportConfig.configurationAt(CONFKEY_DECODING)), e);
		} catch (JSONException e) {
			log.error("Converting the retrieved data into a JSON structure failed. The data was {}", response, e);
		}
	}

	@Override
	public boolean hasName(String providerName) {
		return providerName.equalsIgnoreCase("serverpluginexporter")
				|| providerName.equals(getClass().getCanonicalName());
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
