package de.julielab.concepts.db;

import java.io.IOException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;

import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.java.utilities.CLIInteractionUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class BoltConceptInserter implements ConceptInsertionService {

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




}
