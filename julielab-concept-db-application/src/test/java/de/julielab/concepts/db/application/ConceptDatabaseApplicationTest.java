package de.julielab.concepts.db.application;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class ConceptDatabaseApplicationTest {
	private static final String TESTCONFIG = "src/test/resources/testconfig.xml";
	private static XMLConfiguration configuration;

	@BeforeClass
	public static void setup() throws ConfigurationException, IOException {
		// All we do in the long next lines is to read the test configuration file, get
		// the path to the test database and delete it.
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> configBuilder = new FileBasedConfigurationBuilder<>(
				XMLConfiguration.class).configure(params.xml().setFileName(TESTCONFIG));
		configuration = configBuilder.getConfiguration();
	}

	@AfterClass
	public static void shutdown() throws IOException {
		FileConnectionService databaseService = FileConnectionService.getInstance();
		databaseService.shutdown();
		// For the test, the URI will always be a relative file path
		File dbPath = new File(configuration
				.getString(ConceptDatabaseApplication.CONFKEY_CONNECTION + "." + FileConnectionService.CONFKEY_URI));
		FileUtils.deleteDirectory(dbPath);
	}

	@Test
	public void testConceptImport() throws URISyntaxException, ConceptDatabaseConnectionException {
		ConceptDatabaseApplication.main(new String[] { TESTCONFIG });

		// Check if the Plant Ontology has been imported as expected.
		FileConnectionService databaseService = FileConnectionService.getInstance();
		GraphDatabaseService graphdb = databaseService
				.getDatabase(configuration.configurationAt(ConceptDatabaseApplication.CONFKEY_CONNECTION));
		try (Transaction tx = graphdb.beginTx()) {
			List<Node> facets = graphdb.findNodes(FacetManager.FacetLabel.FACET).stream().collect(Collectors.toList());
			assertEquals(1, facets.size());
			Node po = facets.get(0);
			assertEquals("PO", po.getProperty(FacetConstants.PROP_NAME));
			long numConcepts = graphdb.findNodes(ConceptManager.ConceptLabel.CONCEPT).stream().count();
			assertTrue(numConcepts > 0);
		}
	}
}
