package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.VERSION;

public class FileVersioning implements Versioning {

	private static final Logger log = LoggerFactory.getLogger(FileVersioning.class);
	private GraphDatabaseService graphDb;

	@Override
	public void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException {
		String version = versioningConfig.getString(VERSION);
		String existingVersion = getVersion();
		if (null != existingVersion)
			throw new VersioningException("The database already has a version: " + existingVersion);
		try (Transaction tx = graphDb.beginTx()) {
			graphDb.execute(VersioningConstants.CREATE_VERSION,
					Collections.singletonMap(VERSION, version));
			log.info("Created database version node for version {}", version);
			tx.success();
		}
		// Schema and data manipulation transactions must be separated.
		try (Transaction tx = graphDb.beginTx()) {
			graphDb.execute(VersioningConstants.CREATE_UNIQUE_CONSTRAINT);
			log.info("Created UNIQUE constraint on the version node.");
			tx.success();
		}
	}

	@Override
	public String getVersion() throws VersionRetrievalException {
		try (Transaction tx = graphDb.beginTx()) {
			Result result = graphDb.execute(VersioningConstants.GET_VERSION);
			if (result.hasNext()) {
				Map<String, Object> record = result.next();
				return (String) record.get(VERSION);
			}
		}
		return null;
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
		if (graphDb == null)
			throw new ConceptDatabaseConnectionException("Could not create a file database for connection "
					+ ConfigurationUtils.toString(connectionConfiguration));
	}

}
