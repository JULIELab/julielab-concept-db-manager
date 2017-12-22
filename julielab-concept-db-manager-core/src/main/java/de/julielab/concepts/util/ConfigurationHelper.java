package de.julielab.concepts.util;

import static java.util.stream.Collectors.joining;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

/**
 * A few small methods to help with configuration handling.
 * @author faessler
 *
 */
public class ConfigurationHelper {
	public static final String LS = System.getProperty("line.separator");
	
	public static void checkParameters(HierarchicalConfiguration<ImmutableNode> importConfig, String... parameters) throws ConceptCreationException {
		List<String> parameterNotFound = new ArrayList<>();
		for (String parameter : parameters) {
			if (importConfig.getProperty(parameter) == null)
				parameterNotFound.add(parameter);
		}
		if (!parameterNotFound.isEmpty())
			throw new ConceptCreationException("The following required parameters are not set in the configuration:"+LS
					+ parameterNotFound.stream().collect(joining(LS)));
	}
	
	public static void checkFilesExist(HierarchicalConfiguration<ImmutableNode> importConfig, String... parameters) throws ConceptCreationException {
		checkParameters(importConfig, parameters);
		List<String> parameterNotFound = new ArrayList<>();
		for (String parameter : parameters) {
			if (!new File(importConfig.getString(parameter)).exists())
				parameterNotFound.add(parameter);
		}
		if (!parameterNotFound.isEmpty())
			throw new ConceptCreationException("The following required files given by the configuration do not exist: "+LS
					+ parameterNotFound.stream().collect(joining(LS)));
	}
	
	private ConfigurationHelper() {}

	public static XMLConfiguration loadXmlConfiguration(File configurationFile) throws ConfigurationException {
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> configBuilder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class).configure(params.xml().setFile(configurationFile));
		return configBuilder.getConfiguration();
	}
}
