package de.julielab.concepts.db.core;

import static de.julielab.concepts.db.core.VersioningConstants.GET_VERSION;
import static de.julielab.concepts.db.core.VersioningConstants.PROP_VERSION;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;

public class BoltVersioning implements Versioning {

	private static final Logger log = LoggerFactory.getLogger(BoltVersioning.class);
	private Driver driver;

	@Override
	public void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException {
		String version = versioningConfig.getString(VersioningConstants.CONFKEY_VERSION);
		String existingVersion = getVersion();
		if (null != existingVersion)
			throw new VersioningException("The database already has a version: " + existingVersion);
		try (Session session = driver.session()) {
			try (Transaction tx = session.beginTransaction()) {
				tx.run(new Statement(VersioningConstants.CREATE_VERSION,
						Collections.singletonMap(VersioningConstants.PROP_VERSION, version)));
				log.info("Created database version node for version {}", version);
				tx.success();
			}
			// Schema and data manipulation transactions must be separated.
			try (Transaction tx = session.beginTransaction()) {
				tx.run(new Statement(VersioningConstants.CREATE_UNIQUE_CONSTRAINT));
				log.info("Created UNIQUE constraint on the version node.");
				tx.success();
			}
		}
	}

	@Override
	public String getVersion() throws VersionRetrievalException {
		try (Session session = driver.session(); Transaction tx = session.beginTransaction()) {
			StatementResult result = tx.run(new Statement(GET_VERSION));
			if (result.hasNext()) {
				Record record = result.single();
				return record.get(PROP_VERSION).asString();
			}
			return null;
		}
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		try {
			driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
		} catch (IOException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

}