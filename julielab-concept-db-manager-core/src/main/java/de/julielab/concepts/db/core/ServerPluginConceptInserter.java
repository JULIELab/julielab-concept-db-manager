package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
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
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.checkParameters;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class ServerPluginConceptInserter implements ConceptInserter {
    public static final int CONCEPT_IMPORT_BATCH_SIZE = 1000;
    private static final Logger log = LoggerFactory.getLogger(ServerPluginConceptInserter.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    @Override
    public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfig, ImportConcepts concepts)
            throws ConceptInsertionException {
        try {
            checkParameters(importConfig, slash(SERVER_PLUGIN_INSERTER, PLUGIN_NAME), slash(SERVER_PLUGIN_INSERTER, PLUGIN_ENDPOINT));

            ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
            jsonMapper.setSerializationInclusion(Include.NON_NULL);
            jsonMapper.setSerializationInclusion(Include.NON_EMPTY);

            String serverUri = connectionConfiguration.getString(URI);
            String pluginName = importConfig.getString(slash(SERVER_PLUGIN_INSERTER, PLUGIN_NAME));
            String pluginEndpoint = importConfig.getString(slash(SERVER_PLUGIN_INSERTER, PLUGIN_ENDPOINT));
            HttpConnectionService httpService = HttpConnectionService.getInstance();
            HttpPost httpPost = httpService.getHttpPostRequest(connectionConfiguration, serverUri + String
                    .format(ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint));

            // We first create the facet. While this can be done in a single request together with the concepts
            // to be imported, we want to import the concepts batch-wise. This requires knowledge of the target
            // facet ID. Because when we don't specify the facet ID, a new facet would be created with each
            // batch of concepts.
            String facetId = createFacet(concepts, jsonMapper, httpPost);

            ImportFacet createdFacet = new ImportFacet(facetId);
            String createdFacetJson = jsonMapper.writeValueAsString(createdFacet);
            Map<String, String> dataMap = new HashMap<>();
            StringWriter sw = new StringWriter();
            List<ImportConcept> importConcepts = concepts.getConcepts();
            sw.write("[");
            for (int i = 0; i < importConcepts.size(); i++) {
                ImportConcept importConcept = importConcepts.get(i);
                jsonMapper.writeValue(sw, importConcept);
                if (i > 0 && (i % CONCEPT_IMPORT_BATCH_SIZE == 0 || i == importConcepts.size() - 1)) {
                    sw.write("]");
                    dataMap.put(ConceptManager.KEY_CONCEPTS, sw.getBuffer().toString());
                    dataMap.put(ConceptManager.KEY_FACET, createdFacetJson);
                    if (concepts.getImportOptions() != null)
                        dataMap.put(ConceptManager.KEY_IMPORT_OPTIONS,
                                jsonMapper.writeValueAsString(concepts.getImportOptions()));
                    httpPost.setEntity(new StringEntity(jsonMapper.writeValueAsString(dataMap)));

                    String response = HttpConnectionService.getInstance().sendRequest(httpPost);
                    if (log.isDebugEnabled())
                        log.debug("Server plugin response to concept insertion for batch {}/{}: {}", (i / CONCEPT_IMPORT_BATCH_SIZE), importConcepts.size()/CONCEPT_IMPORT_BATCH_SIZE,  response);
                    sw.getBuffer().delete(0, sw.getBuffer().length());
                    sw.write("[");
                } else {
                    sw.write(",");
                }
            }
            //dataMap.put(ConceptManager.KEY_CONCEPTS, jsonMapper.writeValueAsString(concepts.getConcepts()));
        } catch (InternalNeo4jException e) {
            final ObjectMapper om = new ObjectMapper();
            om.enable(SerializationFeature.INDENT_OUTPUT);
            try {
                log.error("Error report from Neo4j server when trying to insert concepts: {}", om.writeValueAsString(e.getErrorResponse()));
            } catch (JsonProcessingException e1) {
                log.error("Subsequent error occurred when trying to print the internal JSON error: ", e1);
            }
            throw new ConceptInsertionException(e.getErrorResponse().getMessage());
        } catch
        (ConceptDatabaseConnectionException | JsonProcessingException | UnsupportedEncodingException
                        e) {
            throw new ConceptInsertionException(e);
        } catch (ConfigurationException e) {
            log.error("Configuration error occured with configuration {} {}", ConfigurationUtilities.LS, ConfigurationUtils.toString(importConfig));
            throw new ConceptInsertionException(e);
        } catch (IOException e) {
            log.error("Could not convert (parts of) ImportConcepts to JSON", e);
            throw new ConceptInsertionException(e);
        }
    }

    private String createFacet(ImportConcepts concepts, ObjectMapper jsonMapper, HttpPost httpPost) throws ConceptDatabaseConnectionException, IOException {
        Map<String, String> facetDataMap = new HashMap<>();
        facetDataMap.put(ConceptManager.KEY_FACET, jsonMapper.writeValueAsString(concepts.getFacet()));
        httpPost.setEntity(new StringEntity(jsonMapper.writeValueAsString(facetDataMap)));
        String facetCreationResponse = HttpConnectionService.getInstance().sendRequest(httpPost);
        log.debug("Server plugin response to facet creation: {}", facetCreationResponse);
        Map<?, ?> response = jsonMapper.readValue(facetCreationResponse, Map.class);
        return (String) response.get(ConceptManager.KEY_FACET_ID);
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
