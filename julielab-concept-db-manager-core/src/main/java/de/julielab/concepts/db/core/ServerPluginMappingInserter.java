package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ServerPluginMappingInserter extends ServerPluginCallBase implements MappingInserter {
    private final static Logger log = LoggerFactory.getLogger(ServerPluginMappingInserter.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public ServerPluginMappingInserter() {
        super(log);
    }

    @Override
    public void insertMappings(Stream<ImportMapping> mappings) throws MappingInsertionException {
        try {
            ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
            jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

            String serverUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
            String pluginName = ConceptManager.class.getSimpleName();
            String pluginEndpoint = ConceptManager.INSERT_MAPPINGS;
            HttpConnectionService httpService = HttpConnectionService.getInstance();
            HttpPost httpPost = httpService.getHttpPostRequest(connectionConfiguration, serverUri + String
                    .format(ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT, pluginName, pluginEndpoint));
            Map<String, String> dataMap = new HashMap<>();
            dataMap.put(ConceptManager.KEY_MAPPINGS, jsonMapper.writeValueAsString(mappings));
            httpPost.setEntity(new StringEntity(jsonMapper.writeValueAsString(dataMap)));

            String response = HttpConnectionService.getInstance().sendRequest(httpPost);
            log.debug("Server plugin response to mapping insertion: {}", response);
        } catch (ConceptDatabaseConnectionException | JsonProcessingException | UnsupportedEncodingException
                e) {
            throw new MappingInsertionException(e);
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
