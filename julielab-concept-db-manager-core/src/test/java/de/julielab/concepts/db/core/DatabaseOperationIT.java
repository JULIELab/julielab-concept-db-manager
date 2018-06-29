package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.*;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;
import static org.assertj.core.api.Assertions.assertThat;

@Test(suiteName = "integration-tests", dependsOnGroups = "fruit-concepts")
public class DatabaseOperationIT {

    private final static Logger log = LoggerFactory.getLogger(DatabaseOperationIT.class);


    @Test
    public void testAggregation() throws Exception {
        // In this test, we will insert two concepts and a mapping between the two. We then
        // let the ConceptManager plugin create the aggregate for the two concepts.

        XMLConfiguration config = ConfigurationUtilities.
                loadXmlConfiguration(new File("src/test/resources/dboperationconfig.xml"));
        config.setProperty(dot(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        HierarchicalConfiguration<ImmutableNode> connectionConfig =
                config.configurationAt(CONNECTION);


        ImportMapping importMapping = new ImportMapping("id1", "id2", "EQUAL");
        ConceptInsertionService conceptInsertion = ConceptInsertionService.getInstance(connectionConfig);
        MappingInsertionService mappingInsertion = MappingInsertionService.getInstance(connectionConfig);
        DatabaseOperationService dbOperation = DatabaseOperationService.getInstance(connectionConfig);

        mappingInsertion.insertMappings(Stream.of(importMapping));
        dbOperation.operate(config.configurationAt(dot(OPERATIONS, OPERATION)));

        config.setProperty(dot(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        try (Session session = driver.session()) {
            StatementResult result = session.readTransaction(tx -> tx.run("MATCH (a:MAPPING_AGGREGATE) RETURN COUNT(a) AS count"));
            assertThat(result.single().asMap()).containsEntry("count", 1L);
        }
    }

    @Test(dependsOnMethods = "testAggregation")
    public void testCypherOperator() throws ConfigurationException, DatabaseOperationException, IOException, ConceptDatabaseConnectionException {
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/boltoperationconfig.xml"));
        config.setProperty(dot(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));

        // Before the usage of the operator, we do not expect a value for the testprop property
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        String propQuery = "MATCH (c:CONCEPT) WHERE 'id1' IN c.sourceIds RETURN c.testprop";
        StatementResult result = driver.session().readTransaction(tx -> tx.run(propQuery));
        assertThat(result.next().get(0).hasType(InternalTypeSystem.TYPE_SYSTEM.NULL()));

        // Apply the operation configuration
        DatabaseOperationService operationService = DatabaseOperationService.getInstance(config.configurationAt(CONNECTION));
        operationService.operate(config.configurationAt(dot(OPERATIONS, OPERATION)));

        // And now there should be a number
        result = driver.session().readTransaction(tx -> tx.run(propQuery));
        Record record = result.next();
        assertThat(record.get(0).hasType(InternalTypeSystem.TYPE_SYSTEM.NUMBER()));
        assertThat(record.get(0).asInt()).isEqualTo(42);
    }
}
