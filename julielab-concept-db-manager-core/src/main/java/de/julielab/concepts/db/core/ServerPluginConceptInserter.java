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
            Map<String, String> dataMap = new HashMap<>();
            StringWriter sw = new StringWriter();
            List<ImportConcept> importConcepts = concepts.getConcepts();
            for (int i = 0; i < importConcepts.size(); i++) {
                ImportConcept importConcept = importConcepts.get(i);
                try {
                    jsonMapper.writeValue(sw, importConcept);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                importConcepts.set(i, null);
            }
            //dataMap.put(ConceptManager.KEY_CONCEPTS, jsonMapper.writeValueAsString(concepts.getConcepts()));
            dataMap.put(ConceptManager.KEY_CONCEPTS, sw.getBuffer().toString());
            dataMap.put(ConceptManager.KEY_FACET, jsonMapper.writeValueAsString(concepts.getFacet()));
            if (concepts.getImportOptions() != null)
                dataMap.put(ConceptManager.KEY_IMPORT_OPTIONS,
                        jsonMapper.writeValueAsString(concepts.getImportOptions()));
            httpPost.setEntity(new StringEntity(jsonMapper.writeValueAsString(dataMap)));

            String response = HttpConnectionService.getInstance().sendRequest(httpPost);
            log.debug("Server plugin response to concept insertion: {}", response);
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
        }
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
