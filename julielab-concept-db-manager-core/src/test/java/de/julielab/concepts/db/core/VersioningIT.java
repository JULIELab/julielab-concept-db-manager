package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersioningException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.testng.AssertJUnit.assertEquals;

@Test(suiteName = "integration-tests")
public class VersioningIT {


    private static final Logger log = LoggerFactory.getLogger(VersioningIT.class);
    private Driver driver;

    @BeforeClass
    public void loadConfig() throws ConfigurationException, IOException, ConceptDatabaseConnectionException {
        XMLConfiguration configuration = ConfigurationUtilities
                .loadXmlConfiguration(new File("src/test/resources/boltversioningconfig.xml"));
        configuration.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);
        driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
    }

    @AfterMethod
    public void setupTest() {
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                log.debug("Deleting existent VERSION node");
                tx.run("MATCH (v:VERSION) delete v");
                tx.commit();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("DROP CONSTRAINT ON (v:VERSION) ASSERT v." + VERSION + " IS UNIQUE");
                tx.commit();
            }
        }
    }

    public void testHttp() throws ConfigurationException, VersioningException {
        log.debug("Running HTTP test");
        XMLConfiguration configuration = ConfigurationUtilities
                .loadXmlConfiguration(new File("src/test/resources/httpversioningconfig.xml"));
        configuration.setProperty(slash(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);
        HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
        VersioningService instance = VersioningService.getInstance(connectionConfiguration);
        instance.setVersion(versioningConfig);

        assertEquals("1.0-http", instance.getVersion());
    }

    public void testBolt() throws ConfigurationException, VersioningException {
        log.debug("Running BOLT test");
        XMLConfiguration configuration = ConfigurationUtilities
                .loadXmlConfiguration(new File("src/test/resources/boltversioningconfig.xml"));
        configuration.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);
        HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
        VersioningService instance = VersioningService.getInstance(connectionConfiguration);
        instance.setVersion(versioningConfig);

        assertEquals("1.0-bolt", instance.getVersion());
    }
}
