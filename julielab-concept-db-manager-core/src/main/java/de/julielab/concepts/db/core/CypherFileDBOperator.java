package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CypherFileDBOperator implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(CypherFileDBOperator.class);
    private DatabaseManagementService dbms;

    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration) throws IncompatibleActionHandlerConnectionException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(REQUEST, CYPHER_QUERY), operationConfiguration::getString);
            log.info("Sending Cypher query {} to Neo4j embedded database", cypherQuery);
            try (Transaction tx = dbms.database(DEFAULT_DATABASE_NAME).beginTx()) {
                Result result = tx.execute(cypherQuery);
                log.info("Neo4j response: "+ System.getProperty("line.separator") + result.resultAsString());
                tx.commit();
            }
            log.info("Done.");
        }
        catch (ConfigurationException e) {
            throw new IncompatibleActionHandlerConnectionException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        dbms = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "CypherOperator";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, OPERATOR, REQUEST, CYPHER_QUERY), "");
    }
}
