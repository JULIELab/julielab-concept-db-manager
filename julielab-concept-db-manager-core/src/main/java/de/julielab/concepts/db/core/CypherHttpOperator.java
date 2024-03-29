package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.http.*;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.util.stream.Collectors.joining;

public class CypherHttpOperator implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(CypherHttpOperator.class);
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
    private HttpConnectionService httpService;

    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration) throws DatabaseOperationException, IncompatibleActionHandlerConnectionException {
        try {
            String cypherQuery = ConfigurationUtilities.requirePresent(slash(REQUEST, CYPHER_QUERY), operationConfiguration::getString);
            log.info("Sending Cypher query {} to Neo4j via HTTP", cypherQuery);
            Statements statements = new Statements(
                    new Statement(cypherQuery));
            String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
            String transactionalUri = baseUri + Constants.TRANSACTION_ENDPOINT;
            try {
                Response response = httpService.sendStatements(statements, transactionalUri, connectionConfiguration);
                if (!response.getErrors().isEmpty())
                    throw new DatabaseOperationException(
                            "Error happened when trying perform operation: " + response.getErrors());
                List<String> responseLines = new ArrayList<>();
                Iterator<Result> resIt = response.getResults().iterator();
                while (resIt.hasNext()) {
                    Result result = resIt.next();
                    for (Data data : result.getData()) {
                        responseLines.add(data.getRow().stream().map(Object::toString).collect(joining("\t")));
                    }
                }
                log.info("Neo4j response: {}", responseLines.stream().collect(joining(System.getProperty("line.separator"))));
            } catch (ConceptDatabaseConnectionException | IOException e) {
                throw new DatabaseOperationException(e);
            }

            log.info("Done.");
        } catch (ConfigurationException e) {
            throw new IncompatibleActionHandlerConnectionException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        httpService = HttpConnectionService.getInstance();
        // Check if there will be an error thrown due to an invalid URI or something.
        httpService.getHttpRequest(connectionConfiguration, HttpMethod.GET);
        this.connectionConfiguration = connectionConfiguration;
    }

    @Override
    public String getName() {
        return "CypherOperator";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, OPERATOR, OPERATOR), "de.julielab.concepts.db.core.CypherHttpOperator");
        template.addProperty(slash(basePath, OPERATOR, REQUEST, CYPHER_QUERY), "");
    }
}
