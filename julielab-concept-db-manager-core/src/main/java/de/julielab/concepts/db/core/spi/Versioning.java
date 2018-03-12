package de.julielab.concepts.db.core.spi;

import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;

public interface Versioning  {
	void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException;
	String getVersion() throws VersionRetrievalException;
	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException;
}
