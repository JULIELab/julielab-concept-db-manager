package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.concepts.util.InternalNeo4jException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.ProgressBar;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.checkParameters;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.neo4j.plugins.datarepresentation.ImportConcepts.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RestConceptInserter implements ConceptInserter {
    public static final int CONCEPT_IMPORT_BATCH_SIZE = 1000;
    private static final Logger log = LoggerFactory.getLogger(RestConceptInserter.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    @Override
    public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfig, ImportConcepts concepts)
            throws ConceptInsertionException {
        try {
            checkParameters(importConfig, slash(REST, REST_ENDPOINT));

            ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
            jsonMapper.setSerializationInclusion(Include.NON_NULL);
            jsonMapper.setSerializationInclusion(Include.NON_EMPTY);

            String serverUri = connectionConfiguration.getString(URI);
            String pluginEndpoint = importConfig.getString(slash(REST, REST_ENDPOINT));
            HttpConnectionService httpService = HttpConnectionService.getInstance();

            String uri = serverUri + (pluginEndpoint.startsWith("/") ? pluginEndpoint : "/" + pluginEndpoint);
            HttpPost httpPost = (HttpPost) httpService.getHttpRequest(connectionConfiguration, uri, HttpMethod.POST);
            httpPost.addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpPost.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

            PipedOutputStream jsonOut = new PipedOutputStream();
            PipedInputStream entityStream = new PipedInputStream(jsonOut);
            JsonFactory jf = new JsonFactory(jsonMapper);
            JsonGenerator g = jf.createGenerator(jsonOut);
            g.writeStartObject();
            g.writeObjectField(NAME_FACET, concepts.getFacet());
            g.writeObjectField(NAME_IMPORT_OPTIONS, concepts.getImportOptions());
            g.writeNumberField(NAME_NUM_CONCEPTS, concepts.getNumConcepts());

            Thread concept2json = new Thread(() -> {
                try {
                    ProgressBar progressBar = new ProgressBar(concepts.getNumConcepts());
                    Stream<ImportConcept> importConcepts = concepts.getConcepts();
                    g.writeFieldName(NAME_CONCEPTS);
                    g.writeStartArray();
                    for (ImportConcept concept : (Iterable<ImportConcept>) importConcepts::iterator) {
                        g.writeObject(concept);
                        progressBar.incrementDone(true);
                    }
                    g.writeEndArray();
                    g.writeEndObject();
                    g.close();
                } catch (IOException e) {
                    log.error("An error occurred while writing concepts into the output stream.", e);
                    throw new IllegalStateException(new ConceptInsertionException(e));
                }
            });
            concept2json.start();
            httpPost.setEntity(new InputStreamEntity(entityStream));
            log.info("Sending data to server");
            String response = IOUtils.toString(HttpConnectionService.getInstance().sendRequest(httpPost), UTF_8);
            if (log.isDebugEnabled())
                log.debug("Server plugin response to concept insertion: {}", response);
            log.info("Done with concept import.");
        } catch (InternalNeo4jException e) {
            final ObjectMapper om = new ObjectMapper();
            om.enable(SerializationFeature.INDENT_OUTPUT);
            try {
                log.error("Error report from Neo4j server when trying to insert concepts: {}", om.writeValueAsString(e.getErrorResponse()));
            } catch (JsonProcessingException e1) {
                log.error("Subsequent error occurred when trying to print the internal JSON error: ", e1);
            }
            throw new ConceptInsertionException(e.getErrorResponse().getMessage());
        } catch (ConceptDatabaseConnectionException | JsonProcessingException | UnsupportedEncodingException e) {
            throw new ConceptInsertionException(e);
        } catch (ConfigurationException e) {
            log.error("Configuration error occured with configuration {} {}", ConfigurationUtilities.LS, ConfigurationUtils.toString(importConfig));
            throw new ConceptInsertionException(e);
        } catch (IOException e) {
            log.error("Could not convert (parts of) ImportConcepts to JSON", e);
            throw new ConceptInsertionException(e);
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
