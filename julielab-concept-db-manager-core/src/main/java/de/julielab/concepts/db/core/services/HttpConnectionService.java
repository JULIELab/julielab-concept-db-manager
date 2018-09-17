package de.julielab.concepts.db.core.services;

import static de.julielab.concepts.db.core.services.NetworkConnectionCredentials.CONFKEY_PASSW;
import static de.julielab.concepts.db.core.services.NetworkConnectionCredentials.CONFKEY_URI;
import static de.julielab.concepts.db.core.services.NetworkConnectionCredentials.CONFKEY_USER;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.gson.Gson;
import de.julielab.concepts.util.InternalNeo4jException;
import de.julielab.concepts.util.Neo4jServerErrorResponse;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.julielab.concepts.db.core.http.Response;
import de.julielab.concepts.db.core.http.Statements;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;

public class HttpConnectionService {

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

	public HttpPost getHttpPostRequest(HierarchicalConfiguration<ImmutableNode> connectionConfiguration, String httpUri)
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
			HttpPost httpPost = new HttpPost(uri);
			if (authorizationToken != null)
				httpPost.addHeader("Authorization", authorizationToken);
			return httpPost;
		} catch (IllegalArgumentException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

	public HttpPost getHttpPostRequest(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		return getHttpPostRequest(connectionConfiguration, null);
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
		try {
			HttpResponse response = client.execute(request);
			HttpEntity entity = response.getEntity();
			// We take all 200 values with us, because 204 is not really an
			// error. To get specific return codes, see HttpStatus
			// constants.
			if (response.getStatusLine().getStatusCode() < 300) {
				return entity != null ? EntityUtils.toString(entity) : "<no response from Neo4j>";
			}
			String responseString = EntityUtils.toString(entity);
			ObjectMapper om = new ObjectMapper();
			Neo4jServerErrorResponse errorResponse = om.readValue(responseString, Neo4jServerErrorResponse.class);
			throw new InternalNeo4jException(errorResponse);
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
		HttpPost httpPost = httpService.getHttpPostRequest(connectionConfiguration, transactionalUri);
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
