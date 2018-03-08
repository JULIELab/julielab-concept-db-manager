package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.db.core.FunctionCallBase;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.CONFIGURATION;
import static de.julielab.concepts.db.core.ConfigurationConstants.PARAMETERS;
import static de.julielab.concepts.db.core.ConfigurationConstants.dot;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_ENDPOINT;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_NAME;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT;

public class ServerPluginCallBase extends FunctionCallBase{

    private Logger log;

    public ServerPluginCallBase(Logger log) {
        super(log);
    }

    public String callNeo4jServerPlugin(HierarchicalConfiguration<ImmutableNode> connectionConfig, HierarchicalConfiguration<ImmutableNode> methodCallConfig)
            throws ConceptDatabaseConnectionException, MethodCallException {
        String baseUri = connectionConfig.getString(NetworkConnectionCredentials.CONFKEY_URI);
        String pluginName = methodCallConfig.getString(CONFKEY_PLUGIN_NAME);
        String pluginEndpoint = methodCallConfig.getString(CONFKEY_PLUGIN_ENDPOINT);
        Map<String, Object> parameters;
        try {
            HierarchicalConfiguration<ImmutableNode> parameterConfiguration = methodCallConfig
                    .configurationAt(dot(CONFIGURATION, PARAMETERS));
            Map<String, FunctionCallBase.Parameter> parameterMap = null;
            parameterMap = parseParameters(parameterConfiguration);
            parameters = parameterMap.values().stream()
                    .collect(Collectors.toMap(FunctionCallBase.Parameter::getName, FunctionCallBase.Parameter::getRequestValue));
        } catch (MethodCallException e) {
            throw new MethodCallException(e);
        }

        HttpConnectionService httpService = HttpConnectionService.getInstance();
        String completePluginEndpointUri = baseUri + String.format(SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint);
        HttpPost request = httpService.getHttpPostRequest(connectionConfig, completePluginEndpointUri);
        Gson gson = new Gson();
        String response = null;
        try {
            String parameterJson = gson.toJson(parameters);
            request.setEntity(new StringEntity(parameterJson));
            log.info("Sending request {} to {}", parameterJson, completePluginEndpointUri);
            return httpService.sendRequest(request);
        } catch (UnsupportedEncodingException e) {
            throw new ConceptDatabaseConnectionException(e);
        } catch (ConceptDatabaseConnectionException e) {
            log.error("Connection error when posting parameters {} to plugin {}, endpoint {}", parameters, pluginName,
                    pluginEndpoint);
            throw e;
        }
    }
}
