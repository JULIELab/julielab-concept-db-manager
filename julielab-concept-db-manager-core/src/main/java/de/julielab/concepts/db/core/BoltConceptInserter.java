package de.julielab.concepts.db.core;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;

import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.java.utilities.CLIInteractionUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class BoltConceptInserter implements ConceptInserter {

	public static final String CONFKEY_URI = "uri";
	public static final String CONFKEY_USER = "user";
	public static final String CONFKEY_PASSW = "password";

	
	@Override
	public void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException {
		// TODO Auto-generated method stub
		
	}
	
	private String aquirePasswordInteractively(String user) throws IOException {
		return CLIInteractionUtilities.readLineFromStdInWithMessage("Please specify the Neo4j database password for the user \"" + user + "\": ");
	}

	@Override
	public boolean setConfiguration(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws URISyntaxException {
		// TODO Auto-generated method stub
		return false;
	}




}