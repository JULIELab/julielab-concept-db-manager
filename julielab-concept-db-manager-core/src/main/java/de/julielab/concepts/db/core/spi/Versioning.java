package de.julielab.concepts.db.core.spi;

import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;

import static de.julielab.concepts.db.core.ConfigurationConstants.VERSION;
import static de.julielab.jssf.commons.Configurations.slash;

public interface Versioning extends ParameterExposing {
	void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException;
	String getVersion() throws VersionRetrievalException;
	void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException;

	@Override
	default void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
		template.setProperty(slash(basePath, VERSION), 1.0);
	}
}
