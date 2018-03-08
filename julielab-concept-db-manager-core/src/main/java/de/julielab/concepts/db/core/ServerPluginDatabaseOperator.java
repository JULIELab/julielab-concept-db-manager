package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerPluginDatabaseOperator extends ServerPluginCallBase implements DatabaseOperator{
private final static Logger log = LoggerFactory.getLogger(ServerPluginDatabaseOperator.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public ServerPluginDatabaseOperator() {
        super(log);
    }

    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfigration) throws DatabaseOperationException {
        try {
            String response = callNeo4jServerPlugin(connectionConfiguration, operationConfigration);
            log.info("Response from Neo4j: {}", response);
        } catch (ConceptDatabaseConnectionException | MethodCallException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        try {
            HttpConnectionService httpService = HttpConnectionService.getInstance();
            // Check if there will be an error thrown due to an invalid URI or something.
            httpService.getHttpPostRequest(connectionConfiguration);
            this.connectionConfiguration = connectionConfiguration;
        } catch (ConceptDatabaseConnectionException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    @Override
    public boolean hasName(String providername) {
        return providername.equalsIgnoreCase("serverplugindatabaseoperator") || providername.equals(getClass().getCanonicalName());
    }
}
