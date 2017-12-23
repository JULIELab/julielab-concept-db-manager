package de.julielab.concepts.db.core.spi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;

/**
 * <p>
 * Data exporters read a database, extract specific information and store it at
 * some external location in a specific output format.
 * </p>
 * <p>
 * Some complexity arises concerning the connection to the database. Ideally,
 * each data exporter can handle all database connections. The core library
 * delivers connections via BOLT or via direct file access to the database. If a
 * connection can not be obtained, a meaningful exception should be thrown from
 * {@link #setConnection(HierarchicalConfiguration)};
 * </p>
 * 
 * @author faessler
 *
 */
public interface DataExporter {
	/**
	 * Export data from the database to an external location.
	 * 
	 * @param exportConfig
	 *            Export subconfiguration.
	 * @throws ConceptDatabaseConnectionException
	 * @throws DataExportException
	 */
	void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
			throws ConceptDatabaseConnectionException, DataExportException;

	boolean hasName(String providerName);

	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException;

	default String getResourceHeader(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws VersionRetrievalException, IOException {
		String version = VersioningService.getInstance(connectionConfiguration).getVersion();
		StringBuilder sb = new StringBuilder();
		sb.append("# ").append("Concept database version: ").append(version)
				.append(System.getProperty("line.separator"));
		sb.append("# ").append("Concept Database Manager Application version: ").append(getApplicationVersion())
				.append(System.getProperty("line.separator"));
		return sb.toString();
	}

	default String getApplicationVersion() throws IOException {
		BufferedReader br = new BufferedReader(
				new InputStreamReader(getClass().getResourceAsStream("/concept-db-manager-version.txt")));
		return br.readLine().trim();
	}
}
