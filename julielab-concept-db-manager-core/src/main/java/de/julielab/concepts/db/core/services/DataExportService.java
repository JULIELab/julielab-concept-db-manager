package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
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

public class DataExportService implements ParameterExposing {

    private final static Logger log = LoggerFactory.getLogger(DataExportService.class);
    private static Map<HierarchicalConfiguration<ImmutableNode>, DataExportService> serviceMap;
    private ServiceLoader<DataExporter> loader;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    private DataExportService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
        loader = ServiceLoader.load(DataExporter.class);
    }

    /**
     * Returns the data export service singleton specifically created for this
     * passed configuration.
     *
     * @param connectionConfiguration
     * @return
     */
    public static synchronized DataExportService getInstance(
            HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        if (serviceMap == null)
            serviceMap = new HashMap<>();
        return serviceMap.computeIfAbsent(connectionConfiguration, DataExportService::new);
    }

    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
            throws DataExportException {
        Iterator<DataExporter> exporterIt = loader.iterator();
        boolean exporterExecuted = false;
        while (exporterIt.hasNext()) {
            DataExporter exporter = exporterIt.next();
            try {
                exporter.setConnection(connectionConfiguration);
                exporter.exportData(exportConfig);
                exporterExecuted = true;
            } catch (ConceptDatabaseConnectionException e) {
                log.trace("The exporter {} does not support the given connection. Continue search for a compatible exporter.", exporter.getClass().getCanonicalName());
                continue;
            } catch (IncompatibleActionHandlerConnectionException e) {
                log.trace("The exporter {} does not support the given export configuration. Continue search for a compatible exporter.", exporter.getClass().getCanonicalName());
                continue;
            }
            log.debug("Database exporter {} was run for export configuration {} ", exporter.getClass().getCanonicalName(), ConfigurationUtils.toString(exportConfig));
        }
        if (!exporterExecuted)
            throw new DataExportException("No available data exporter was compatible with export configuration " + ConfigurationUtils.toString(exportConfig) + " and connection configuration " + ConfigurationUtils.toString(connectionConfiguration));
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        for (Iterator<DataExporter> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
            DataExporter exporter = operatorIterator.next();
            // this creates a new exports/export path
            template.addProperty(basePath, "");
            exporter.exposeParameters(last(basePath), template);
        }
    }

}
