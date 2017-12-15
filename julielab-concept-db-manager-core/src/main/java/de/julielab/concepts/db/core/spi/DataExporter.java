package de.julielab.concepts.db.core.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;

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
	 */
	void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig);

	boolean hasName(String providerName);

	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException;
}
