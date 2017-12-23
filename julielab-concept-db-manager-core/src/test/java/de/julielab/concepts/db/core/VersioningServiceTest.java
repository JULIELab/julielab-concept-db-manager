package de.julielab.concepts.db.core;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.io.fs.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConfigurationHelper;

public class VersioningServiceTest {
	
	@Before
	@After
	public void setupTest() throws Exception {
		FileUtils.deleteRecursively(new File("src/test/resources/graph.db"));
	}
	
	private static final Logger log = LoggerFactory.getLogger(VersioningServiceTest.class);
	@Test
	public void testFile() throws Exception {
		log.debug("Running File test");
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/fileversioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(RootConfigurationConstants.CONFKEY_CONNECTION);
		HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(RootConfigurationConstants.VERSIONING);
		VersioningService instance = VersioningService.getInstance(connectionConfiguration);
		instance.setVersion(versioningConfig);
		
		assertEquals("1.0-file", instance.getVersion());
	}
}
