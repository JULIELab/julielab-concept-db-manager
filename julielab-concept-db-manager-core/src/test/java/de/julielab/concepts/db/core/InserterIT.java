package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.XMLConfiguration;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

public class InserterIT {

    private final static Logger log = LoggerFactory.getLogger(InserterIT.class);


    @Test(groups = "integration-tests")
    public void testInsertion() throws Exception {
        // This test inserts a single concept
        XMLConfiguration config = new XMLConfiguration();
        // First, setup the configuration
        config.setProperty(dot(CONNECTION, URI), "http://localhost:" + TestSuite.neo4j.getMappedPort(7474));
        config.setProperty(dot(IMPORT, CONFIGURATION, PLUGIN_NAME), "ConceptManager");
        config.setProperty(dot(IMPORT, CONFIGURATION, PLUGIN_ENDPOINT), "insert_concepts");

        // Import the concept
        ConceptInsertionService service = ConceptInsertionService.getInstance(config.configurationAt(CONNECTION));
        ImportConcept concept = new ImportConcept("Apfelsine", Arrays.asList("Orange"), new ConceptCoordinates("id1", "source1", true));
        ImportFacetGroup fg = new ImportFacetGroup("Biologie", 0, Collections.emptyList());
        ImportFacet facet = new ImportFacet(fg, null, "Obst", null, FacetConstants.SRC_TYPE_HIERARCHICAL);
        ImportConcepts concepts = new ImportConcepts(Stream.of(concept), facet);
        service.insertConcepts(config.configurationAt(IMPORT), concepts);

        // Check that the concept has actually been created
        config.setProperty(dot(CONNECTION, URI), "bolt://localhost:" + TestSuite.neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        try (Session s = driver.session()) {
            Map<String, Object> resultMap = s.readTransaction(tx -> {
                StatementResult result = tx.run("MATCH (c:CONCEPT) RETURN COUNT(c) AS count");
                if (result.hasNext())
                    return result.next().asMap();
                return null;
            });
            assertThat(resultMap).isNotNull().containsEntry("count", 1L);
        }
    }
}