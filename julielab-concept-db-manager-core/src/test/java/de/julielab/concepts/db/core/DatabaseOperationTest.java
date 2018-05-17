package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.concepts.db.core.services.DatabaseOperationService;
import de.julielab.concepts.db.core.services.MappingInsertionService;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseOperationTest {

    private final static Logger log = LoggerFactory.getLogger(DatabaseOperationTest.class);

    private static GenericContainer neo4j;

    @BeforeClass
    public static void startNeo4j() {
        neo4j = new GenericContainer("neo4j:3.3.1").
                withEnv("NEO4J_AUTH", "none").withExposedPorts(7474, 7687).
                withClasspathResourceMapping("julielab-neo4j-plugins-concepts-1.8.0-assembly.jar",
                        "/var/lib/neo4j/plugins/julielab-neo4j-plugins-concepts-1.8.0-assembly.jar",
                        BindMode.READ_WRITE);
        neo4j.start();
        Slf4jLogConsumer toStringConsumer = new Slf4jLogConsumer(log);
        neo4j.followOutput(toStringConsumer, OutputFrame.OutputType.STDOUT);
    }

    @AfterClass
    public static void stopNeo4j() {
        neo4j.stop();
    }

    @Test
    public void testAggregation() throws Exception {
        // In this test, we will insert two concepts and a mapping between the two. We then
        // let the ConceptManager plugin create the aggregate for the two concepts.

        XMLConfiguration config = ConfigurationUtilities.
                loadXmlConfiguration(new File("src/test/resources/dboperationconfig.xml"));
        config.setProperty(dot(CONNECTION, URI), "http://localhost:" + neo4j.getMappedPort(7474));
        HierarchicalConfiguration<ImmutableNode> connectionConfig =
                config.configurationAt(CONNECTION);


        ImportConcept c1 = new ImportConcept("concept1",
                new ConceptCoordinates("id1", "source1", true));
        ImportConcept c2 = new ImportConcept("concept2",
                new ConceptCoordinates("id2", "source1", true));

        ImportFacetGroup fg = new ImportFacetGroup("testfg", 0, Collections.emptyList());

        ImportFacet facet = new ImportFacet(fg, null, "myfacet",
                null, FacetConstants.SRC_TYPE_HIERARCHICAL);

        ImportConcepts importConcepts = new ImportConcepts(Stream.of(c1, c2), facet);
        ImportMapping importMapping = new ImportMapping("id1", "id2", "EQUAL");
        ConceptInsertionService conceptInsertion = ConceptInsertionService.getInstance(connectionConfig);
        MappingInsertionService mappingInsertion = MappingInsertionService.getInstance(connectionConfig);
        DatabaseOperationService dbOperation = DatabaseOperationService.getInstance(connectionConfig);

        conceptInsertion.insertConcepts(config.configurationAt(dot(IMPORTS, IMPORT)),
                importConcepts);
        mappingInsertion.insertMappings(Stream.of(importMapping));
        dbOperation.operate(config.configurationAt(dot(OPERATIONS, OPERATION)));

        config.setProperty(dot(CONNECTION, URI), "bolt://localhost:" + neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        try (Session session = driver.session()) {
            StatementResult result = session.readTransaction(tx -> tx.run("MATCH (a:MAPPING_AGGREGATE) RETURN COUNT(a) AS count"));
            assertThat(result.single().asMap()).containsEntry("count", 1L);
        }
    }
}
