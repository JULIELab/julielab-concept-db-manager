package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
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
import java.io.InputStream;
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

    public InputStream callNeo4jRestEndpoint(HierarchicalConfiguration<ImmutableNode> connectionConfig, HierarchicalConfiguration<ImmutableNode> methodCallConfig, String defaultHttpMethod)
            throws ConceptDatabaseConnectionException, MethodCallException, IncompatibleActionHandlerConnectionException {
        String state = "read_connection";
        try {
            java.net.URI baseUri = java.net.URI.create(requirePresent(NetworkConnectionCredentials.CONFKEY_URI, key -> connectionConfig.getString(key)));
            state = "read_methodcallconfig";
            String endpoint = requirePresent(slash(REQUEST, REST, REST_ENDPOINT), key -> methodCallConfig.getString(key));
            String httpMethod = methodCallConfig.getString(slash(REQUEST, REST, HTTP_METHOD), defaultHttpMethod);
            Map<String, Object> parameters = null;
            if (methodCallConfig.getKeys(slash(REQUEST, PARAMETERS)).hasNext()) {
                HierarchicalConfiguration<ImmutableNode> parameterConfiguration = methodCallConfig.configurationAt(slash(REQUEST, PARAMETERS));
                Map<String, Parameter> parameterMap;
                parameterMap = parseParameters(parameterConfiguration);
                parameters = parameterMap.values().stream()
                        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequestValue));
            }

            state = "prepare_request";
            HttpConnectionService httpService = HttpConnectionService.getInstance();
            java.net.URI completePluginEndpointUri = new java.net.URI(baseUri.getScheme(), null, baseUri.getHost(), baseUri.getPort(), endpoint.startsWith("/") ? endpoint : "/" + endpoint, null, null);
            HttpRequestBase request = httpService.getHttpRequest(connectionConfig, completePluginEndpointUri.toString(), httpMethod);
            request.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            Gson gson = new Gson();
            try {
                String parameterJson = null;
                if (parameters != null && !httpMethod.equals(HttpMethod.GET) && !httpMethod.equals(HttpMethod.DELETE)) {
                    parameterJson = gson.toJson(parameters);
                    ((HttpEntityEnclosingRequestBase) request).setEntity(new StringEntity(parameterJson));
                } else if (parameters != null) {
                    String query = parameters.entrySet().stream().map(e -> toQueryParam(e, gson)).collect(Collectors.joining("&"));
                    java.net.URI uriWithQueryParams = new java.net.URI(completePluginEndpointUri.getScheme(), completePluginEndpointUri.getAuthority(), completePluginEndpointUri.getPath(), query, null);
                    request.setURI(uriWithQueryParams);
                }
                state = "send_request";
                log.info("Sending request {} to {}", parameterJson, request.getURI());
                return httpService.sendRequest(request);
            } catch (UnsupportedEncodingException e) {
                throw new ConceptDatabaseConnectionException(e);
            } catch (ConceptDatabaseConnectionException e) {
                log.error("Connection error when posting parameters {} to endpoint {}", parameters, endpoint);
                throw e;
            }
        } catch (ConfigurationException e) {
            log.debug("{} is incompatible with the given configuration in state '{}' with message '{}'", RestCallBase.class.getSimpleName(), state, e.getMessage());
            throw new IncompatibleActionHandlerConnectionException(e);
        } catch (URISyntaxException e) {
            log.error("Could not construct correct request URI.", e);
            throw new IllegalArgumentException(e);
        }
    }

    private String toQueryParam(Map.Entry<String, Object> parameter, Gson gson) {
        Object value = parameter.getValue();
        if (value instanceof Iterable || value.getClass().isArray())
            value = gson.toJson(value);
        return parameter.getKey() + "=" + value;
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, REST_ENDPOINT), "");
        template.addProperty(slash(basePath, REQUEST, PARAMETERS, "parametername"), "value");
        template.addProperty(ws(slash(basePath, REQUEST, PARAMETERS, "parametername"), "@parametername"), "optional: parameter name");
        template.addProperty(slash(basePath, REQUEST, PARAMETERS, "arrayparameter", "arrayitem"), Arrays.asList("value1", "value2"));
        template.addProperty(ws(slash(basePath, REQUEST, PARAMETERS, "arrayparameter"), "@tojson"), "optional: should the parameter be extra JSON encoded and sent as a simple string");
    }
}
