package de.julielab.concepts.db.core.services;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.concepts.db.core.http.Response;
import de.julielab.concepts.db.core.http.Statements;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static de.julielab.concepts.db.core.services.NetworkConnectionCredentials.*;


public class HttpConnectionService {
    private final static Logger log = LoggerFactory.getLogger(HttpConnectionService.class);
    private static HttpConnectionService service;
    private CloseableHttpClient client;

    public HttpConnectionService() {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        client = HttpClients.custom().setConnectionManager(connManager).build();
    }

    public static HttpConnectionService getInstance() {
        if (service == null)
            service = new HttpConnectionService();
        return service;
    }

    public HttpRequestBase getHttpRequest(HierarchicalConfiguration<ImmutableNode> connectionConfiguration, String httpUri, String method)
            throws ConceptDatabaseConnectionException {
        try {
            String uri = connectionConfiguration.getString(CONFKEY_URI);
            if (uri == null)
                throw new ConceptDatabaseConnectionException("No URI was specified in connection configuration "
                        + ConfigurationUtils.toString(connectionConfiguration));
            if (httpUri != null)
                uri = httpUri;
            String user = connectionConfiguration.getString(CONFKEY_USER);
            String password = connectionConfiguration.getString(CONFKEY_PASSW);

            checkForHttpScheme(uri);

            String authorizationToken = user != null && password != null
                    ? "Basic " + Base64.encodeBase64URLSafeString((user + ":" + password).getBytes())
                    : null;
            HttpRequestBase request;
            switch (method) {
                case HttpMethod.GET:
                    request = new HttpGet(uri);
                    break;
                case HttpMethod.POST:
                    request = new HttpPost(uri);
                    break;
                case HttpMethod.PUT:
                    request = new HttpPut(uri);
                    break;
                case HttpMethod.DELETE:
                    request = new HttpDelete(uri);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown HTTP method: " + method);
            }
            if (authorizationToken != null)
                request.addHeader("Authorization", authorizationToken);
            return request;
        } catch (IllegalArgumentException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    public HttpRequestBase getHttpRequest(HierarchicalConfiguration<ImmutableNode> connectionConfiguration, String method) throws ConceptDatabaseConnectionException {
        return getHttpRequest(connectionConfiguration, null, method);
    }

    private void checkForHttpScheme(String httpUri) throws ConceptDatabaseConnectionException {
        try {
            URI uri = new URI(httpUri);
            if (uri.getScheme() == null || !uri.getScheme().equals("http"))
                throw new ConceptDatabaseConnectionException(
                        "The given URI " + httpUri + " does not specify the http scheme.");
        } catch (URISyntaxException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    public String sendRequest(HttpUriRequest request) throws ConceptDatabaseConnectionException {
        String responseString = null;
        try {
            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();
            // We take all 200 values with us, because 204 is not really an
            // error. To get specific return codes, see HttpStatus
            // constants.
            if (response.getStatusLine().getStatusCode() < 300) {
                return entity != null ? EntityUtils.toString(entity) : "<no response from Neo4j>";
            } else if (response.getStatusLine().getStatusCode() == 404)
                throw new IllegalArgumentException("Server returned status code HTTP " + response.getStatusLine().getStatusCode() + " Not Found: " + request.getMethod() + ": " + request.getURI().toString());
            responseString = EntityUtils.toString(entity);
            if (responseString != null && !responseString.isEmpty()) {
                throw new ConceptDatabaseConnectionException(responseString);
            }
            throw new ConceptDatabaseConnectionException(response.getStatusLine().getStatusCode() + ": " + response.getStatusLine().getReasonPhrase());
        } catch (com.fasterxml.jackson.databind.exc.MismatchedInputException e) {
            log.error("Error when trying to deserialize the JSON error message. The original JSON is {}", responseString);
            throw new ConceptDatabaseConnectionException(e);
        } catch (ParseException | IOException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    public Response sendStatements(Statements statements, String transactionalUri,
                                   HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException, IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.setSerializationInclusion(Include.NON_NULL);
        jsonMapper.setSerializationInclusion(Include.NON_EMPTY);

        HttpConnectionService httpService = HttpConnectionService.getInstance();
        HttpPost httpPost = (HttpPost) httpService.getHttpRequest(connectionConfiguration, transactionalUri, "POST");
        // taken from
        // https://neo4j.com/docs/developer-manual/3.3/http-api/#http-api-transactional
        httpPost.addHeader("Accept", "application/json; charset=UTF-8");
        httpPost.addHeader("Content-Type", "application/json");
        String jsonStatements = jsonMapper.writeValueAsString(statements);
        httpPost.setEntity(new StringEntity(jsonStatements));
        String responseString = httpService.sendRequest(httpPost);
        return jsonMapper.readValue(responseString, Response.class);
    }
}
