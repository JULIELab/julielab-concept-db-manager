package de.julielab.concepts.db.core;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;

public class JavaClassExporter extends DataExporterBase {

    public final static String CONFKEY_CONFIGURATION = "configuration";

	private GraphDatabaseService graphDb;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	@Override
	public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
			throws ConceptDatabaseConnectionException, DataExportException {
		String outputFile = exportConfig.getString(CONFKEY_OUTPUT_FILE);
		try {
			String result = callInstanceMethod(exportConfig.configurationAt(CONFKEY_CONFIGURATION), graphDb);
			result = getResourceHeader(connectionConfiguration) + result;
			writeBase64GzipToFile(outputFile, result);
		} catch (MethodCallException | VersionRetrievalException | IOException e) {
			throw new DataExportException(e);
		}
    }


	@Override
	public boolean hasName(String providerName) {
		return providerName.equalsIgnoreCase("javaclassexporter") || providerName.equals(getClass().getCanonicalName());
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		this.connectionConfiguration = connectionConfiguration;
		graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
	}

}
