package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.REST;
import static de.julielab.concepts.db.core.ConfigurationConstants.REST_ENDPOINT;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RestMappingInserter implements MappingInserter {
    private final static Logger log = LoggerFactory.getLogger(RestMappingInserter.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;


    @Override
    public void insertMappings(HierarchicalConfiguration<ImmutableNode> importConfiguration, Stream<ImportMapping> mappings) throws MappingInsertionException {
        try {
            ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
            jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

            String serverUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
            String restEndpoint = importConfiguration.getString(slash(REST, REST_ENDPOINT));
            HttpConnectionService httpService = HttpConnectionService.getInstance();

            String uri = serverUri + (restEndpoint.startsWith("/") ? restEndpoint : "/" + restEndpoint);
            HttpPost httpPost = (HttpPost) httpService.getHttpRequest(connectionConfiguration, uri, HttpMethod.POST);

            PipedOutputStream jsonOut = new PipedOutputStream();
            PipedInputStream entityStream = new PipedInputStream(jsonOut);
            JsonFactory jf = new JsonFactory(jsonMapper);
            JsonGenerator g = jf.createGenerator(jsonOut);
            Thread mapping2json = new Thread(() -> {
                try {
                    g.writeStartArray();
                    for (ImportMapping mapping : (Iterable<ImportMapping>) () -> mappings.iterator())
                        g.writeObject(mapping);
                    g.writeEndArray();
                    g.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            mapping2json.start();
            httpPost.setEntity(new InputStreamEntity(entityStream));
            String response = IOUtils.toString(HttpConnectionService.getInstance().sendRequest(httpPost), UTF_8);
            log.debug("Server plugin response to mapping insertion: {}", response);
        } catch (ConceptDatabaseConnectionException | JsonProcessingException | UnsupportedEncodingException
                e) {
            throw new MappingInsertionException(e);
        } catch (IOException e) {
            log.error("Could not convert (parts of) ImportMappings to JSON", e);
            throw new MappingInsertionException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        HttpConnectionService httpService = HttpConnectionService.getInstance();
        // Check if there will be an error thrown due to an invalid URI or something.
        httpService.getHttpRequest(connectionConfiguration, HttpMethod.GET);
        this.connectionConfiguration = connectionConfiguration;
    }
}
