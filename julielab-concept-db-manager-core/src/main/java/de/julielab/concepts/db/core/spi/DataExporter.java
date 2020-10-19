package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
import de.julielab.jssf.commons.spi.ExtensionPoint;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

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
public interface DataExporter extends ExtensionPoint, DatabaseConnected, ParameterExposing {
	/**
	 * Export data from the database to an external location.
	 * 
	 * @param exportConfig
	 *            Export subconfiguration.
	 * @throws ConceptDatabaseConnectionException When this exporter cannot connect to the database.
	 * @throws DataExportException When the implementing exporter can not serve the provided export configuration.
	 */
	void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
            throws ConceptDatabaseConnectionException, DataExportException, IncompatibleActionHandlerConnectionException;
}
