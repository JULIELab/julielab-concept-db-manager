package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.DatabaseOperationException;
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

public class CypherFileDBExporter implements DataExporter {
    private final static Logger log = LoggerFactory.getLogger(CypherFileDBExporter.class);
    private GraphDatabaseService graphDb;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

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
        template.addProperty(slash(basePath, OPERATOR, OPERATOR), "de.julielab.concepts.db.core.CypherFileDBOperator");
        template.addProperty(slash(basePath, OPERATOR, CONFIGURATION, CYPHER_QUERY), "");
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws ConceptDatabaseConnectionException, DataExportException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(CONFIGURATION, CYPHER_QUERY), exportConfig::getString);
            String outputPath = ConfigurationUtilities.requirePresent(slash(CONFIGURATION, OUTPUT_FILE), exportConfig::getString);
            log.info("Sending Cypher query {} to Neo4j embedded database", cypherQuery);
            List<String> outputLines = new ArrayList<>();
            try (Transaction tx = graphDb.beginTx()) {
                Result result = graphDb.execute(cypherQuery);
                while (result.hasNext()) {
                    Map<String, Object> resultMap = result.next();
                    outputLines.add(resultMap.values().stream().map(v -> v.toString()).collect(Collectors.joining("\t")));
                }
                tx.success();
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
