package de.julielab.concepts.db.application;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersioningException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.CONNECTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class ConceptDatabaseApplicationTest {
	private static final String SIMPLEIMPORT = "src/test/resources/simpleimport.xml";
	private static final String NAMEDOPERATIONS = "src/test/resources/namedoperations.xml";
	private static XMLConfiguration configuration;

	@BeforeClass
	public static void setup() throws ConfigurationException, IOException {
		// All we do in the long next lines is to read the test configuration file, get
		// the path to the test database and delete it.
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> configBuilder = new FileBasedConfigurationBuilder<>(
				XMLConfiguration.class).configure(params.xml().setFileName(SIMPLEIMPORT));
		configuration = configBuilder.getConfiguration();
	}

	@AfterClass
	public static void shutdown() throws IOException {
		FileConnectionService databaseService = FileConnectionService.getInstance();
		databaseService.shutdown();
		// For the test, the URI will always be a relative file path
		File dbPath = new File(configuration
				.getString(CONNECTION + "." + FileConnectionService.CONFKEY_URI));
		FileUtils.deleteDirectory(dbPath);
	}

	@Test
	public void testConceptImport() throws URISyntaxException, ConceptDatabaseConnectionException, DataExportException, VersioningException, CmdLineException {
		ConceptDatabaseApplication.main(new String[] { "--import", "-c", SIMPLEIMPORT, "-nv"});

		// Check if the Plant Ontology has been imported as expected.
		FileConnectionService databaseService = FileConnectionService.getInstance();
		GraphDatabaseService graphdb = databaseService
				.getDatabase(configuration.configurationAt(CONNECTION));
		try (Transaction tx = graphdb.beginTx()) {
			List<Node> facets = graphdb.findNodes(FacetManager.FacetLabel.FACET).stream().collect(Collectors.toList());
			assertEquals(1, facets.size());
			Node po = facets.get(0);
			assertEquals("PO", po.getProperty(FacetConstants.PROP_NAME));
			long numConcepts = graphdb.findNodes(ConceptManager.ConceptLabel.CONCEPT).stream().count();
			assertTrue(numConcepts > 0);
		}
	}

	@Test(dependsOnMethods = "testConceptImport")
	public void testNamedOperations() throws ConceptDatabaseConnectionException, DataExportException, VersioningException, ConfigurationException {
	    // The configuration sets the property "myprop" on the concept "anther wall".

        // First, clear the property we add as a test.
        GraphDatabaseService graphDb = FileConnectionService.getInstance().getDatabase(configuration.configurationAt(CONNECTION));
        try (Transaction tx = graphDb.beginTx()) {
            graphDb.execute("MATCH (c:CONCEPT) REMOVE c.myprop");
            tx.success();
        }

		ConceptDatabaseApplication.main(new String[] { "--operation", "antherwall", "non-existent", "-c", NAMEDOPERATIONS, "-nv"});

        XMLConfiguration configuration = ConfigurationUtilities.loadXmlConfiguration(new File(NAMEDOPERATIONS));
        try (Transaction tx = graphDb.beginTx()) {
            Result result = graphDb.execute("MATCH (c:CONCEPT) WHERE c.myprop IS NOT NULL return COUNT(c) as count");
            assertThat(result.hasNext()).isTrue();
            assertThat((long) result.next().get("count")).isEqualTo(1);
        }
    }

    @Test(dependsOnMethods = "testConceptImport")
    public void testNamedOperationsAll() throws ConceptDatabaseConnectionException, DataExportException, VersioningException, ConfigurationException {
        // The same as testNamedOperations but with the --all option

        // First, clear the property we add as a test.
        GraphDatabaseService graphDb = FileConnectionService.getInstance().getDatabase(configuration.configurationAt(CONNECTION));
        try (Transaction tx = graphDb.beginTx()) {
            graphDb.execute("MATCH (c:CONCEPT) REMOVE c.myprop");
            tx.success();
        }

        ConceptDatabaseApplication.main(new String[] { "--all", "antherwall", "non-existent", "-c", NAMEDOPERATIONS, "-nv"});

        XMLConfiguration configuration = ConfigurationUtilities.loadXmlConfiguration(new File(NAMEDOPERATIONS));
        try (Transaction tx = graphDb.beginTx()) {
            Result result = graphDb.execute("MATCH (c:CONCEPT) WHERE c.myprop IS NOT NULL return COUNT(c) as count");
            assertThat(result.hasNext()).isTrue();
            assertThat((long) result.next().get("count")).isEqualTo(1);
        }
    }
}
