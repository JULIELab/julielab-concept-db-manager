package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.CONFIGURATION;
import static de.julielab.concepts.db.core.ConfigurationConstants.CYPHER_QUERY;
import static de.julielab.concepts.db.core.ConfigurationConstants.OPERATOR;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class CypherFileDBOperator implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(CypherFileDBOperator.class);
    private GraphDatabaseService graphDb;

    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfigration) throws DatabaseOperationException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(OPERATOR, CONFIGURATION, CYPHER_QUERY), operationConfigration::getString);
            log.info("Sending Cypher query {} to Neo4j embedded database", cypherQuery);
            try (Transaction tx = graphDb.beginTx()) {
                Result result = graphDb.execute(cypherQuery);
                List<String> responseLines = new ArrayList<>();
                while (result.hasNext()) {
                    Map<String, Object> record = result.next();
                    responseLines.add(record.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining("\t")));
                }
                log.info("Neo4j response: " + responseLines.stream().collect(Collectors.joining("\t")));
            }
            log.info("Done.");
        }
        catch (ConfigurationException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "CypherFileDBOperator";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, OPERATOR, OPERATOR), "de.julielab.concepts.db.core.CypherFileDBOperator");
        template.addProperty(slash(basePath, OPERATOR, CONFIGURATION, CYPHER_QUERY), "");
    }
}
