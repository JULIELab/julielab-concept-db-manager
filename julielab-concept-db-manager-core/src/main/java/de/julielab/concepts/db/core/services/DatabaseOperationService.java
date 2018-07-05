package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import static de.julielab.concepts.db.core.ConfigurationConstants.OPERATOR;
import static de.julielab.concepts.db.core.ConfigurationConstants.URI;
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
        boolean operatorFound = false;
        boolean operatorExecuted = false;
        log.trace("Operation Service called.");
        try {
            String operatorName = ConfigurationUtilities.requirePresent(OPERATOR, operationConfiguration::getString);
            log.trace("Operator {} was demanded", operatorName);
            for (Iterator<DatabaseOperator> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
                DatabaseOperator operator = operatorIterator.next();
                log.trace("Checking if operator with name {} matches the demanded operator name", operator.getName());
                if (operator.hasName(operatorName)) {
                    operatorFound = true;
                    try {
                        operator.setConnection(connectionConfiguration);
                    } catch (ConceptDatabaseConnectionException e) {
                        log.debug("Database operator " + operator.getClass().getCanonicalName() + " is skipped because it could not serve the connection configuration " + ConfigurationUtils.toString(connectionConfiguration) + ": " + e.getMessage() + ". Looking for another compatible operator.");
                        continue;
                    }
                    operator.operate(operationConfiguration);
                    operatorExecuted = true;
                }
            }
        } catch (ConfigurationException e) {
            throw new DatabaseOperationException(e);
        }
        if (!operatorFound)
            throw new DatabaseOperationException(
                    "Database operation failed because no operator for the connection configuration "
                            + connectionConfiguration.getString(URI)
                            + " was found. Make sure that an appropriate operator provider is given in the META-INF/services/"
                            + DatabaseOperator.class.getCanonicalName() + " file.");
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
