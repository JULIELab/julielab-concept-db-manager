package de.julielab.concepts.db.core;

import java.io.IOException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class BoltConceptInserter implements ConceptInserter {

	
	private Driver driver;

	@Override
	public void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException {
		try (Session session = driver.session(); Transaction tx = session.beginTransaction()){
			// TODO perform insertion
		}
	}
	
	@Override
	public boolean setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		try {
			driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
			return true;
		} catch (IOException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}




}
