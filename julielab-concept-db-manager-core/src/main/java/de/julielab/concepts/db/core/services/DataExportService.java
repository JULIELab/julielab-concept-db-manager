package de.julielab.concepts.db.core.services;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;

public class DataExportService {
	public static final String CONFKEY_EXPORTER = "dataexporter";

	private ServiceLoader<DataExporter> loader;
	private static DataExportService service;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	private DataExportService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		loader = ServiceLoader.load(DataExporter.class);
	}

	public static DataExportService getInstance(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		if (service == null)
			service = new DataExportService(connectionConfiguration);
		return service;
	}

	public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws DataExportException, ConceptDatabaseConnectionException, URISyntaxException {
		Iterator<DataExporter> exporterIt = loader.iterator();
		String exporterName = exportConfig.getString(CONFKEY_EXPORTER);
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

}
