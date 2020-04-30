package de.julielab.concepts.db.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

public class FileDatabaseMappingInserter extends JavaMethodCallBase implements MappingInserter {

    private final static Logger log = LoggerFactory.getLogger(FileDatabaseMappingInserter.class);
    private GraphDatabaseService graphDb;

    public FileDatabaseMappingInserter() {
        super(log);
    }

    @Override
    public void insertMappings(Stream<ImportMapping> mappings) throws MappingInsertionException {
        if (graphDb == null)
            throw new MappingInsertionException(
                    "No access to a file-based graph database. " +
                            "The FileDatabaseMappingInserter has not been initialized properly. " +
                            "Call setConfiguration() and check for thrown exceptions before calling this method.");
        ConceptManager cm = new ConceptManager();
        ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
        jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        try {
            if (log.isInfoEnabled())
                log.info("Inserting mappings into file based Neo4j database");
            cm.insertMappings(graphDb, jsonMapper.writeValueAsString(mappings));
        } catch (IOException e) {
            log.error("Could not translate mapping into JSON.");
            throw new MappingInsertionException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException {
        graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
        if (graphDb == null)
            throw new ConceptDatabaseConnectionException("Could not create a file database for connection "
                    + ConfigurationUtils.toString(connectionConfiguration));
    }
}
