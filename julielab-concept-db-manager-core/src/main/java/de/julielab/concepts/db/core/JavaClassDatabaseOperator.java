package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;

public class JavaClassDatabaseOperator extends FunctionCallBase implements DatabaseOperator {

    public final static String CONFKEY_CONFIGURATION = "configuration";
    private GraphDatabaseService graphDb;


    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration) throws DatabaseOperationException {
        try {
            callInstanceMethod(operationConfiguration.configurationAt(CONFKEY_CONFIGURATION), graphDb);
        } catch (MethodCallException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
    }

    @Override
    public boolean hasName(String providername) {
        return providername.equalsIgnoreCase("javaclassdatabaseoperator") || providername.equals(getClass().getCanonicalName());
    }
}
