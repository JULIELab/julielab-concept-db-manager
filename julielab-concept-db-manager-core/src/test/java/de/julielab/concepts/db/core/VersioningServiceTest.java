package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.jssf.commons.Configurations;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.io.fs.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static de.julielab.concepts.db.core.ConfigurationConstants.CONNECTION;
import static de.julielab.concepts.db.core.ConfigurationConstants.VERSIONING;
import static org.junit.Assert.assertEquals;

public class VersioningServiceTest {
	
	@BeforeClass
	@AfterClass
	public void setupTest() throws Exception {
		FileUtils.deleteRecursively(new File("src/test/resources/graph.db"));
	}
	
	private static final Logger log = LoggerFactory.getLogger(VersioningServiceTest.class);

	@Test
	public void testFile() throws Exception {
		log.debug("Running File test");
		XMLConfiguration configuration = Configurations
				.loadXmlConfiguration(new File("src/test/resources/fileversioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(CONNECTION);
		HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
		VersioningService instance = VersioningService.getInstance(connectionConfiguration);
		instance.setVersion(versioningConfig);
		
		assertEquals("1.0-file", instance.getVersion());
	}
}
