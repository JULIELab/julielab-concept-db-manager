package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;

@Test(suiteName = "integration-tests", groups = "fruit-concepts")
public class InserterIT {

    private final static Logger log = LoggerFactory.getLogger(InserterIT.class);

    public void testInsertion() throws Exception {
        // This test inserts a single concept
        XMLConfiguration config = new XMLConfiguration();
        config.setExpressionEngine(new XPathExpressionEngine());
        // First, setup the configuration
        config.setProperty(slash(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        config.setProperty(slash(IMPORT, SERVER_PLUGIN_INSERTER, PLUGIN_ENDPOINT), "/concepts/"+ConceptManager.CM_REST_ENDPOINT+"/"+ConceptManager.INSERT_CONCEPTS);

        // Import the concepts
        ConceptInsertionService service = ConceptInsertionService.getInstance(config.configurationAt(CONNECTION));
        ImportConcept concept1 = new ImportConcept("Apfelsine", Arrays.asList("Orange"), new ConceptCoordinates("id1", "source1", true));
        ImportConcept concept2 = new ImportConcept("Birne", Collections.emptyList(), new ConceptCoordinates("id2", "source1", true));
        ImportFacetGroup fg = new ImportFacetGroup("Biologie", 0, Collections.emptyList());
        ImportFacet facet = new ImportFacet(fg, null, "Obst", null, FacetConstants.SRC_TYPE_HIERARCHICAL);
        ImportConcepts concepts = new ImportConcepts(Stream.of(concept1, concept2), facet);
        service.insertConcepts(config.configurationAt(IMPORT), concepts);

        // Check that the concept has actually been created
        config.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        try (Session s = driver.session()) {
            Map<String, Object> resultMap = s.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:CONCEPT) RETURN COUNT(c) AS count");
                if (result.hasNext())
                    return result.next().asMap();
                return null;
            });
            assertThat(resultMap).isNotNull().containsEntry("count", 2L);
        }
    }
}
