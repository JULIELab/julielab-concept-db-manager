import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.RootConfigurationConstants;
import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConfigurationHelper;
import de.julielab.concepts.util.VersioningException;

public class VersioningTest {

	
	private static final Logger log = LoggerFactory.getLogger(VersioningTest.class);
	
	@Before
	@After
	public void setupTest() throws Exception {
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/boltversioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(RootConfigurationConstants.CONFKEY_CONNECTION);
		Driver driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
		try (Session session = driver.session(); Transaction tx = session.beginTransaction()) {
			log.debug("Deleting existent VERSION node");
			tx.run(new Statement("MATCH (v:VERSION) delete v"));
			tx.success();
		}
	}
	
	@Test
	public void testHttp() throws ConfigurationException, VersioningException {
		log.debug("Running HTTP test");
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/httpversioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(RootConfigurationConstants.CONFKEY_CONNECTION);
		HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(RootConfigurationConstants.VERSIONING);
		VersioningService instance = VersioningService.getInstance(connectionConfiguration);
		instance.setVersion(versioningConfig);
		
		assertEquals("1.0-http", instance.getVersion(connectionConfiguration));
	}
	
	@Test
	public void testBolt() throws ConfigurationException, VersioningException {
		log.debug("Running BOLT test");
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/boltversioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(RootConfigurationConstants.CONFKEY_CONNECTION);
		HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(RootConfigurationConstants.VERSIONING);
		VersioningService instance = VersioningService.getInstance(connectionConfiguration);
		instance.setVersion(versioningConfig);
		
		assertEquals("1.0-bolt", instance.getVersion(connectionConfiguration));
	}
}
