import java.io.File;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.Test;

import de.julielab.concepts.db.core.RootConfigurationConstants;
import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConfigurationHelper;
import de.julielab.concepts.util.VersioningException;

public class VersioningTest {
	@Test
	public void testHttp() throws ConfigurationException, VersioningException {
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/versioningconfig.xml"));
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(RootConfigurationConstants.CONFKEY_CONNECTION);
		String version = configuration.getString(RootConfigurationConstants.VERSION);
		VersioningService instance = VersioningService.getInstance(connectionConfiguration);
		instance.setVersion(version);
	}
}
