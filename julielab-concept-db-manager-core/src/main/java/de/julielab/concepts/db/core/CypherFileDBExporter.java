package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CypherFileDBExporter extends DataExporterImpl {
    private final static Logger log = LoggerFactory.getLogger(CypherFileDBExporter.class);
    private DatabaseManagementService dbms;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public CypherFileDBExporter() {
        super(log);
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        dbms = FileConnectionService.getInstance().getDatabaseManagementService(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "CypherExporter";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, REQUEST, CYPHER_QUERY), "");
        template.addProperty(slash(basePath, OUTPUT_FILE), "");
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws DataExportException, IncompatibleActionHandlerConnectionException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(REQUEST, CYPHER_QUERY), exportConfig::getString);
            String outputPath = ConfigurationUtilities.requirePresent(OUTPUT_FILE, exportConfig::getString);
            log.info("Sending Cypher query {} to Neo4j embedded database", cypherQuery);
            List<String> outputLines = new ArrayList<>();
            try (Transaction tx = dbms.database(DEFAULT_DATABASE_NAME).beginTx()) {
                Result result = tx.execute(cypherQuery);
                while (result.hasNext()) {
                    Map<String, Object> resultMap = result.next();
                    outputLines.add(resultMap.values().stream().map(Object::toString).collect(Collectors.joining("\t")));
                }
            }
            writeData(new File(outputPath),
                    getResourceHeader(connectionConfiguration),
                    new ByteArrayInputStream(outputLines.stream().collect(Collectors.joining(System.getProperty("line.separator"))).getBytes(UTF_8)));
            log.info("Done.");
        } catch (ConfigurationException e) {
            throw new IncompatibleActionHandlerConnectionException(e);
        } catch (VersionRetrievalException | IOException e) {
            throw new DataExportException(e);
        }
    }
}
