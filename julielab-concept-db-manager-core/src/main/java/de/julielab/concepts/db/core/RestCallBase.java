package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.*;

public abstract class RestCallBase extends FunctionCallBase {


    public RestCallBase(Logger log) {
        super(log);
    }

    public String callNeo4jServerPlugin(HierarchicalConfiguration<ImmutableNode> connectionConfig, HierarchicalConfiguration<ImmutableNode> methodCallConfig, String defaultHttpMethod)
            throws ConceptDatabaseConnectionException, MethodCallException {
        try {
            java.net.URI baseUri = java.net.URI.create(requirePresent(NetworkConnectionCredentials.CONFKEY_URI, key -> connectionConfig.getString(key)));
            String endpoint = requirePresent(slash(REST, REST_ENDPOINT), key -> methodCallConfig.getString(key));
            String httpMethod = methodCallConfig.getString(HTTP_METHOD, defaultHttpMethod);
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
            // Convention: Is the plugin name given, this is a legacy Server Plugin. Otherwise, it is an
            // unmanaged extension.
            java.net.URI completePluginEndpointUri = new java.net.URI(baseUri.getScheme(), null, baseUri.getHost(), baseUri.getPort(), endpoint.startsWith("/") ? endpoint : "/" + endpoint, null, null);
            HttpRequestBase request = httpService.getHttpRequest(connectionConfig, completePluginEndpointUri.toString(), httpMethod);
            request.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            Gson gson = new Gson();
            try {
                String parameterJson = null;
                if (parameters != null && !httpMethod.equals(HttpMethod.GET)) {
                    parameterJson = gson.toJson(parameters);
                    ((HttpEntityEnclosingRequestBase) request).setEntity(new StringEntity(parameterJson));
                } else {
                    String query = parameters.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
                    java.net.URI uriWithQueryParams = new java.net.URI(completePluginEndpointUri.getScheme(), completePluginEndpointUri.getAuthority(), completePluginEndpointUri.getPath(), query, null);
                    request.setURI(uriWithQueryParams);
                }
                log.info("Sending request {} to {}", parameterJson, completePluginEndpointUri);
                return httpService.sendRequest(request);
            } catch (UnsupportedEncodingException e) {
                throw new ConceptDatabaseConnectionException(e);
            } catch (ConceptDatabaseConnectionException e) {
                log.error("Connection error when posting parameters {} to endpoint {}", parameters, endpoint);
                throw e;
            }
        } catch (ConfigurationException e) {
            throw new ConceptDatabaseConnectionException(e);
        } catch (URISyntaxException e) {
            log.error("Could not construct correct request URI.", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, REST_ENDPOINT), "");
        template.addProperty(slash(basePath, CONFIGURATION, PARAMETERS, "parametername"), "value");
        template.addProperty(ws(slash(basePath, CONFIGURATION, PARAMETERS, "parametername"), "@parametername"), "optional: parameter name");
        template.addProperty(slash(basePath, CONFIGURATION, PARAMETERS, "arrayparameter", "arrayitem"), Arrays.asList("value1", "value2"));
        template.addProperty(ws(slash(basePath, CONFIGURATION, PARAMETERS, "arrayparameter"), "@tojson"), "optional: should the parameter be extra JSON encoded and sent as a simple string");
    }
}
