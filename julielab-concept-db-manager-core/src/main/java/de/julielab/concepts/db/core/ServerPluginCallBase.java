package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MethodCallException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT;
import static de.julielab.java.utilities.ConfigurationUtilities.*;

public abstract class ServerPluginCallBase extends FunctionCallBase {


    public ServerPluginCallBase(Logger log) {
        super(log);
    }

    public String callNeo4jServerPlugin(HierarchicalConfiguration<ImmutableNode> connectionConfig, HierarchicalConfiguration<ImmutableNode> methodCallConfig)
            throws ConceptDatabaseConnectionException, MethodCallException {
        try {
            String baseUri = requirePresent(NetworkConnectionCredentials.CONFKEY_URI, key -> connectionConfig.getString(key));
            String pluginName = requirePresent(slash(CONFIGURATION, PLUGIN_NAME), key -> methodCallConfig.getString(key));
            String pluginEndpoint = requirePresent(slash(CONFIGURATION, PLUGIN_ENDPOINT), key -> methodCallConfig.getString(key));
            Map<String, Object> parameters = null;
            if (methodCallConfig.getKeys(slash(CONFIGURATION, PARAMETERS)).hasNext()) {
                try {
                    HierarchicalConfiguration<ImmutableNode> parameterConfiguration = methodCallConfig.configurationAt(slash(CONFIGURATION, PARAMETERS));
                    Map<String, Parameter> parameterMap;
                    parameterMap = parseParameters(parameterConfiguration);
                    parameters = parameterMap.values().stream()
                            .collect(Collectors.toMap(Parameter::getName, Parameter::getRequestValue));
                } catch (MethodCallException e) {
                    throw new MethodCallException(e);
                }
            }

            HttpConnectionService httpService = HttpConnectionService.getInstance();
            String completePluginEndpointUri = baseUri + String.format(SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint);
            HttpPost request = httpService.getHttpPostRequest(connectionConfig, completePluginEndpointUri);
            Gson gson = new Gson();
            try {
                String parameterJson = null;
                if (parameters != null) {
                    parameterJson = gson.toJson(parameters);
                    request.setEntity(new StringEntity(parameterJson));
                }
                log.info("Sending request {} to {}", parameterJson, completePluginEndpointUri);
                return httpService.sendRequest(request);
            } catch (UnsupportedEncodingException e) {
                throw new ConceptDatabaseConnectionException(e);
            } catch (ConceptDatabaseConnectionException e) {
                log.error("Connection error when posting parameters {} to plugin {}, endpoint {}", parameters, pluginName,
                        pluginEndpoint);
                throw e;
            }
        } catch (ConfigurationException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, CONFIGURATION, PLUGIN_NAME), "");
        template.addProperty(slash(basePath, CONFIGURATION, PLUGIN_ENDPOINT), "");
        template.addProperty(slash(basePath, CONFIGURATION, PARAMETERS, "parametername"), "value");
        template.addProperty(ws(slash(basePath, CONFIGURATION, PARAMETERS, "parametername"), "@parametername"), "optional: parameter name");
        template.addProperty(slash(basePath, CONFIGURATION, PARAMETERS, "arrayparameter", "arrayitem"), Arrays.asList("value1", "value2"));
        template.addProperty(ws(slash(basePath, CONFIGURATION, PARAMETERS, "arrayparameter"), "@tojson"), "optional: should the parameter be extra JSON encoded and sent as a simple string");
    }
}
