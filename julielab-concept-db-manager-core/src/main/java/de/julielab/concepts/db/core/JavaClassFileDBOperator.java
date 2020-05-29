package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.julielab.concepts.db.core.ConfigurationConstants.REQUEST;

public class JavaClassFileDBOperator extends JavaMethodCallBase implements DatabaseOperator {
    private final static Logger log = LoggerFactory.getLogger(JavaClassFileDBOperator.class);
    private DatabaseManagementService dbms;

    public JavaClassFileDBOperator() {
        super(log);
    }


    @Override
    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration) throws DatabaseOperationException {
        try {
            callInstanceMethod(operationConfiguration.configurationAt(REQUEST), dbms);
        } catch (MethodCallException e) {
            throw new DatabaseOperationException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        dbms = FileConnectionService.getInstance().getDatabaseManagementService(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "JavaClassFileDBOperator";
    }

}
