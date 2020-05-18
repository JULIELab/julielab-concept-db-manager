package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;

public class JavaClassFileDBOperator extends JavaMethodCallBase implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(JavaClassFileDBOperator.class);
    private GraphDatabaseService graphDb;

    public JavaClassFileDBOperator() {
        super(log);
    }


    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration) throws DatabaseOperationException {
        try {
            callInstanceMethod(operationConfiguration.configurationAt(CONFIGURATION), graphDb);
        } catch (MethodCallException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "JavaClassFileDBOperator";
    }

}
