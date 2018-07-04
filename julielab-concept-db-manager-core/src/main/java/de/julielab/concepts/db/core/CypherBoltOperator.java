package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

/**
 * Sends a given Cypher query and writes the retrieved results into the given output file. One record per line,
 * fields are tab-separated, all field values are converted to strings.
 */
public class CypherBoltOperator implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(CypherBoltOperator.class);

    private Driver driver;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> exportConfig) throws DatabaseOperationException {
        try {
            String query = ConfigurationUtilities.requirePresent(slash(CONFIGURATION, CYPHER_QUERY), exportConfig::getString);

            log.info("Sending Cypher statement {} to Neo4j", query);
            try (Session session = driver.session()) {
                StatementResult result = session.writeTransaction(tx -> tx.run(query));
                List<String> responseLines = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    for (int i = 0; i < record.size(); i++) {
                        Value val = record.get(i);
                        if (val.hasType(TYPE_SYSTEM.STRING()))
                            responseLines.add(val.asString());
                        else if (val.hasType(TYPE_SYSTEM.NODE()))
                            responseLines.add(val.asNode().asMap().entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining(", ")));
                        else if (val.hasType(TYPE_SYSTEM.NUMBER()))
                            responseLines.add(val.asNumber().toString());
                        else responseLines.add("<return value currently not supported>");
                    }
                }
                log.info("Neo4j response: " + responseLines.stream().collect(Collectors.joining("\t")));
            }
            log.info("Done.");
        } catch (ConfigurationException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        try {
            driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
        } catch (IOException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    @Override
    public String getName() {
        return "CypherOperator";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, CONFIGURATION, CYPHER_QUERY), "");
        template.addProperty(slash(basePath, CONFIGURATION, OUTPUT_FILE), "");
    }
}
