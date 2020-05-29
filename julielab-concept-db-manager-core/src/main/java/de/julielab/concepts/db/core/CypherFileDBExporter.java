package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class CypherFileDBExporter extends DataExporterImpl {
    private final static Logger log = LoggerFactory.getLogger(CypherFileDBExporter.class);
    private GraphDatabaseService graphDb;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public CypherFileDBExporter() {
        super(log);
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "CypherExporter";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, EXPORTER), getName());
        template.addProperty(slash(basePath, REQUEST, CYPHER_QUERY), "");
        template.addProperty(slash(basePath, REQUEST, OUTPUT_FILE), "");
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws DataExportException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(REQUEST, CYPHER_QUERY), exportConfig::getString);
            String outputPath = ConfigurationUtilities.requirePresent(slash(REQUEST, OUTPUT_FILE), exportConfig::getString);
            log.info("Sending Cypher query {} to Neo4j embedded database", cypherQuery);
            List<String> outputLines = new ArrayList<>();
            try (Transaction tx = graphDb.beginTx()) {
                Result result = tx.execute(cypherQuery);
                while (result.hasNext()) {
                    Map<String, Object> resultMap = result.next();
                    outputLines.add(resultMap.values().stream().map(Object::toString).collect(Collectors.joining("\t")));
                }
            }
            writeData(new File(outputPath),
                    getResourceHeader(connectionConfiguration),
                    outputLines.stream().collect(Collectors.joining(System.getProperty("line.separator"))));
            log.info("Done.");
        } catch (ConfigurationException | VersionRetrievalException | IOException e) {
            throw new DataExportException(e);
        }
    }
}
