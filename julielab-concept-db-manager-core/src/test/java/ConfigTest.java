import java.util.Iterator;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;

public class ConfigTest {
	public static void main(String[] args) throws Exception {
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<XMLConfiguration>(
				XMLConfiguration.class).configure(params.xml().setFileName("src/test/resources/testconfig.xml"));
		XMLConfiguration config = builder.getConfiguration();
		for (String key : (Iterable<String>) config::getKeys) {
			System.out.println(key);
		}
	}
}
