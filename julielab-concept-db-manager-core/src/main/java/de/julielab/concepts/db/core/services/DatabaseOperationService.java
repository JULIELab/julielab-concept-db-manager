package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import static de.julielab.java.utilities.ConfigurationUtilities.last;

public class DatabaseOperationService implements ParameterExposing {

    private final static Logger log = LoggerFactory.getLogger(DatabaseOperationService.class);
    private static Map<HierarchicalConfiguration<ImmutableNode>, DatabaseOperationService> serviceMap;
    private ServiceLoader<DatabaseOperator> loader;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public DatabaseOperationService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
        this.loader = ServiceLoader.load(DatabaseOperator.class);
    }

    /**
     * Returns the data export service singleton specifically created for this
     * passed configuration.
     *
     * @param connectionConfiguration
     * @return
     */
    public static synchronized DatabaseOperationService getInstance(
            HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        if (serviceMap == null)
            serviceMap = new HashMap<>();
        return serviceMap.computeIfAbsent(connectionConfiguration, DatabaseOperationService::new);
    }

    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfiguration)
            throws DatabaseOperationException {
        boolean operatorExecuted = false;
        log.trace("Operation Service called.");
        for (Iterator<DatabaseOperator> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
            DatabaseOperator operator = operatorIterator.next();
            try {
                operator.setConnection(connectionConfiguration);
                operator.operate(operationConfiguration);
                operatorExecuted = true;
            } catch (ConceptDatabaseConnectionException e) {
                log.trace("Database operator " + operator.getClass().getCanonicalName() + " is skipped because it could not serve the connection configuration " + ConfigurationUtils.toString(connectionConfiguration) + ": " + e.getMessage() + ". Looking for another compatible operator.");
                continue;
            }
            log.debug("Database operator {} was run for operation {}", operator.getClass().getCanonicalName(), ConfigurationUtils.toString(operationConfiguration));
        }
        if (!operatorExecuted)
            throw new DatabaseOperationException("No available data operator was compatible with operation configuration " + ConfigurationUtils.toString(operationConfiguration) + " and connection configuration " + ConfigurationUtils.toString(connectionConfiguration));
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        for (Iterator<DatabaseOperator> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
            DatabaseOperator operator = operatorIterator.next();
            // this creates a new operations/operation path
            template.addProperty(basePath, "");
            operator.exposeParameters(last(basePath), template);
        }
    }
}
