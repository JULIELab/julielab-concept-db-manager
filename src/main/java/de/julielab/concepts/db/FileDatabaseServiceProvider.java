package de.julielab.concepts.db;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class FileDatabaseServiceProvider implements FileDatabaseService {
	public static final String CONFKEY_URI = "uri";
	private GraphDatabaseService graphdb;

	public FileDatabaseServiceProvider(HierarchicalConfiguration<ImmutableNode> dbConnectionConfiguration)
			throws URISyntaxException {
		GraphDatabaseFactory graphDatabaseFactory = new GraphDatabaseFactory();
		URI dbUri = new URI(dbConnectionConfiguration.getString(CONFKEY_URI));
		graphdb = graphDatabaseFactory.newEmbeddedDatabase(new File(dbUri));
	}

	@Override
	public GraphDatabaseService getFileDatabase() {
		return graphdb;
	}

	@Override
	public void closeDatabase() {
		graphdb.shutdown();
	}
}
