package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import static de.julielab.concepts.db.core.ConfigurationConstants.EXPORTER;
import static de.julielab.java.utilities.ConfigurationUtilities.last;

public class DataExportService implements ParameterExposing{

	private ServiceLoader<DataExporter> loader;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private static Map<HierarchicalConfiguration<ImmutableNode>, DataExportService> serviceMap;

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
			throws DataExportException, ConceptDatabaseConnectionException {
		Iterator<DataExporter> exporterIt = loader.iterator();
		String exporterName = exportConfig.getString(EXPORTER);
		if (exporterName == null)
			throw new ConceptDatabaseConnectionException(
					"The name of the exporter was not given. It must be given by the configuration name "
							+ EXPORTER);
		boolean exporterFound = false;
		while (exporterIt.hasNext()) {
			DataExporter exporter = exporterIt.next();
			if (exporter.hasName(exporterName)) {
				exporter.setConnection(connectionConfiguration);
				exporter.exportData(exportConfig);
				exporterFound = true;
			}
		}
		if (!exporterFound)
			throw new DataExportException("No data exporter with name " + exporterName
					+ " was found. Make sure that the desired data exporter is on the classpath and registered via the META-INF/services/de.julielab.concepts.db.core.spi.DataExporter file.");
	}

	@Override
	public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
		for (Iterator<DataExporter> operatorIterator = loader.iterator(); operatorIterator.hasNext(); ) {
            DataExporter exporter = operatorIterator.next();
			template.addProperty(basePath, "");
			exporter.exposeParameters(last(basePath), template);
		}
	}

}
