package de.julielab.concepts.db.application;

import java.io.File;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.services.ConceptCreationService;
import de.julielab.concepts.db.services.ConceptInsertionService;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class ConceptDatabaseApplication {

	private static final Logger log = LoggerFactory.getLogger(ConceptDatabaseApplication.class);

	public static final String CONFKEY_IMPORT = "imports.import";
	public static final String CONFKEY_CONNECTION = "connection";

	public static void main(String[] args) {
		if (args.length != 1) {
			log.error("Usage: {} <XML configuration file>", ConceptDatabaseApplication.class.getSimpleName());
			System.exit(1);
		}

		File configFile = new File(args[0]);
		log.debug("Reading configuration from {}", configFile);
		try {
			Parameters params = new Parameters();
			FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<XMLConfiguration>(
					XMLConfiguration.class).configure(params.xml().setFile(configFile));
			XMLConfiguration configuration = builder.getConfiguration();
			run(configuration);
		} catch (ConfigurationException e) {
			throw new IllegalArgumentException(
					"The configuration file " + configFile.getAbsolutePath() + " could not be loaded.", e);
		} catch (ConceptCreationException e) {
			log.error("Database creation could not be completed because the concept creation failed.", e);
		}
	}

	private static void run(XMLConfiguration configuration) throws ConceptCreationException {
		ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
		ConceptInsertionService insertionService = ConceptInsertionService.getInstance(configuration.configurationAt(CONFKEY_CONNECTION));
		List<HierarchicalConfiguration<ImmutableNode>> importConfigs = configuration.configurationsAt(CONFKEY_IMPORT);
		for (HierarchicalConfiguration<ImmutableNode> importConfig : importConfigs) {
			Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfig);
			insertionService.insertConcepts(concepts);
		}
	}

}