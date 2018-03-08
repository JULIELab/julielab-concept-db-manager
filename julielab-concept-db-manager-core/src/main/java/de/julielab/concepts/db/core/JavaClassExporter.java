package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.MethodCallException;
import de.julielab.concepts.util.VersionRetrievalException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JavaClassExporter extends JavaMethodCallBase implements DataExporter {

    private final static Logger log = LoggerFactory.getLogger(JavaClassExporter.class);

    public static final String CONFKEY_CONFIGURATION = "configuration";
    public static final String CONFKEY_OUTPUT_FILE = "configuration.outputfile";

	private GraphDatabaseService graphDb;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public JavaClassExporter() {
        super(log);
    }

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
