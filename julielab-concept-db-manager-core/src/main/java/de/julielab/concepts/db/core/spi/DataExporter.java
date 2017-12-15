package de.julielab.concepts.db.core.spi;

import java.net.URISyntaxException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;

public interface DataExporter {
	/**
	 * Export data from the database to an external location.
	 * @param exportConfig Export subconfiguration.
	 */
	void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig);
	boolean hasName(String providerName);
	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws URISyntaxException, ConceptDatabaseConnectionException;
}
