package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.ConfigurationConstants;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.db.core.spi.DatabaseOperator;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.jssf.commons.Configurations;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.jssf.commons.util.ConfigurationException;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.jssf.commons.Configurations.last;

public class DatabaseOperationService implements ParameterExposing{

    private final static Logger log = LoggerFactory.getLogger(DatabaseOperationService.class);

    private static DatabaseOperationService service;
    private ServiceLoader<DatabaseOperator> loader;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
    private static Map<HierarchicalConfiguration<ImmutableNode>, DatabaseOperationService> serviceMap;

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

    public void operate(HierarchicalConfiguration<ImmutableNode> operationConfigration)
            throws DatabaseOperationException {
        boolean operatorFound = false;
        try {
            String operatorName = Configurations.requirePresent(OPERATOR, operationConfigration::getString);
            for (Iterator<DatabaseOperator> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
                DatabaseOperator operator = operatorIterator.next();
                try {
                    if (operator.hasName(operatorName)) {
                        operator.setConnection(connectionConfiguration);
                        operator.operate(operationConfigration);
                        operatorFound = true;
                    }
                } catch (ConceptDatabaseConnectionException e) {
                    log.debug("Database operator " + operator.getClass().getCanonicalName() + " could not serve the connection configuration " + ConfigurationUtils.toString(connectionConfiguration) + ": " + e.getMessage());
                }
            }
        } catch (ConfigurationException e) {
            throw new DatabaseOperationException(e);
        }
        if (!operatorFound)
            throw new DatabaseOperationException(
                    "Database operation failed because no operator for the connection configuration "
                            + ConfigurationUtils.toString(connectionConfiguration)
                            + " was found. Make sure that an appropriate operator provider is given in the META-INF/services/"
                            + DatabaseOperator.class.getCanonicalName() + " file.");
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        for (Iterator<DatabaseOperator> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
            DatabaseOperator operator = operatorIterator.next();
            template.addProperty(basePath, "");
            operator.exposeParameters(last(basePath), template);
        }
    }
}
