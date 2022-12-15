package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.DatabaseOperationService;
import de.julielab.concepts.db.core.services.MappingInsertionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
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
        config.setProperty(slash(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        HierarchicalConfiguration<ImmutableNode> connectionConfig =
                config.configurationAt(CONNECTION);


        ImportMapping importMapping = new ImportMapping("id1", "id2", "EQUAL");
        MappingInsertionService mappingInsertion = MappingInsertionService.getInstance(connectionConfig);
        DatabaseOperationService dbOperation = DatabaseOperationService.getInstance(connectionConfig);

        // We need an import configuration to tell that we want to use an unmanaged server extension rather
        // than a legacy plugin. We do this by not specifying the plugin name.
        HierarchicalConfiguration<ImmutableNode> mappingImportConfig = ConfigurationUtilities.createEmptyConfiguration();
        mappingImportConfig.setProperty(slash(REST, REST_ENDPOINT), "/concepts/"+ ConceptManager.CM_REST_ENDPOINT+"/"+ConceptManager.INSERT_MAPPINGS);
        mappingInsertion.insertMappings(mappingImportConfig, Stream.of(importMapping));
        dbOperation.operate(config.configurationAt(slash(OPERATIONS, OPERATION)));

        config.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        try (Session session = driver.session()) {
            Map<String, Object> responseMap = session.readTransaction(tx -> tx.run("MATCH (a:MAPPING_AGGREGATE) RETURN COUNT(a) AS count").single().asMap());
            assertThat(responseMap).containsEntry("count", 1L);
        }
    }

    public void testBoltCypherOperator() throws ConfigurationException, DatabaseOperationException, IOException, ConceptDatabaseConnectionException {
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/cypheroperationconfig.xml"));
        config.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));

        // First, clear property we will set for the test
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        driver.session().writeTransaction(tx -> tx.run("MATCH (c:CONCEPT) REMOVE c.testprop"));
        // Check that the property is indeed not set
        String propQuery = "MATCH (c:CONCEPT) WHERE c.sourceIds0 = 'id1' RETURN c.testprop";
        Value result = driver.session().readTransaction(tx -> tx.run(propQuery).next().get(0));
        assertThat(result.hasType(InternalTypeSystem.TYPE_SYSTEM.NULL()));

        // Apply the operation configuration
        DatabaseOperationService operationService = DatabaseOperationService.getInstance(config.configurationAt(CONNECTION));
        operationService.operate(config.configurationAt(slash(OPERATIONS, OPERATION)));

        // And now there should be a number
        result = driver.session().readTransaction(tx -> tx.run(propQuery).next().get(0));
        assertThat(result.hasType(InternalTypeSystem.TYPE_SYSTEM.NUMBER()));
        assertThat(result.asInt()).isEqualTo(42);
    }

    public void testHttpCypherOperator() throws ConfigurationException, DatabaseOperationException, IOException, ConceptDatabaseConnectionException {
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/cypheroperationconfig.xml"));
        // First, clear property we will set for the test. We use a BOLT connection because it's so easy to use
        config.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        Driver driver = BoltConnectionService.getInstance().getBoltDriver(config.configurationAt(CONNECTION));
        driver.session().writeTransaction(tx -> tx.run("MATCH (c:CONCEPT) REMOVE c.testprop"));
        // Check that the property is indeed not set
        String propQuery = "MATCH (c:CONCEPT) WHERE 'id1' IN c.sourceIds0 RETURN c.testprop";
        Value value = driver.session().readTransaction(tx -> tx.run(propQuery).next().get(0));
        assertThat(value.hasType(InternalTypeSystem.TYPE_SYSTEM.NULL()));

        // Now switch to the HTTP connection for the actual test
        config.setProperty(slash(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        // Apply the operation configuration
        DatabaseOperationService operationService = DatabaseOperationService.getInstance(config.configurationAt(CONNECTION));
        operationService.operate(config.configurationAt(slash(OPERATIONS, OPERATION)));

        // And now there should be a number
        value = driver.session().readTransaction(tx -> tx.run(propQuery).next().get(0));
        assertThat(value.hasType(InternalTypeSystem.TYPE_SYSTEM.NUMBER()));
        assertThat(value.asInt()).isEqualTo(42);
    }
}
