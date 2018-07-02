package de.julielab.concepts.db.application;

import de.julielab.concepts.db.core.services.ConceptCreationService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.concepts.db.core.services.DataExportService;
import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.*;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerRegistry;
import org.kohsuke.args4j.ParserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;


public class ConceptDatabaseApplication {

	public enum Task {
		IMPORT, EXPORT, ALL, SET_VERSION
	}

	private static final Logger log = LoggerFactory.getLogger(ConceptDatabaseApplication.class);

	public static void main(String[] args) throws DataExportException, ConceptDatabaseConnectionException, VersioningException {

        OptionHandlerRegistry.getRegistry().registerHandler(String[].class, OptionalStringArrayOptionHandler.class);
		ParserProperties parserProperties = ParserProperties.defaults();
		parserProperties.withUsageWidth(120);
		CLIParameters options = new CLIParameters();
		CmdLineParser parser = new CmdLineParser(options, parserProperties);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            parser.printUsage(System.err);
            System.exit(2);
        }
        if (args.length == 0) {
			parser.printUsage(System.out);
			System.exit(1);
		}


//		Task task = Task.valueOf(args[0].toUpperCase());
//		File configFile = new File(args[1]);
//		log.debug("Reading configuration from {}", configFile);
//		try {
//			Parameters params = new Parameters();
//			FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<XMLConfiguration>(
//					XMLConfiguration.class).configure(params.xml().setFile(configFile));
//			XMLConfiguration configuration = builder.getConfiguration();
//			run(task, configuration);
//		} catch (ConfigurationException e) {
//			throw new IllegalArgumentException(
//					"The configuration file " + configFile.getAbsolutePath() + " could not be loaded.", e);
//		} catch (ConceptCreationException e) {
//			log.error("Database creation could not be completed because the concept creation failed.", e);
//		} catch (FacetCreationException e) {
//			log.error("Database creation could not be completed because the facet creation failed.", e);
//		} catch (ConceptInsertionException e) {
//			log.error("Concept insertion failed", e);
//		}
	}

	private static void run(Task task, XMLConfiguration configuration) throws ConceptCreationException,
			FacetCreationException, ConceptInsertionException, DataExportException, ConceptDatabaseConnectionException, VersioningException {
		HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
				.configurationAt(CONNECTION);
		
		if (task == Task.IMPORT || task == Task.ALL) {
			ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
			ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);
			List<HierarchicalConfiguration<ImmutableNode>> importConfigs = configuration
					.configurationsAt(dot(IMPORTS, IMPORT));
			for (HierarchicalConfiguration<ImmutableNode> importConfig : importConfigs) {
				Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfig);
				insertionService.insertConcepts(importConfig, concepts);
			}
		}
		if (task == Task.SET_VERSION || task == Task.ALL) {
			HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
			VersioningService.getInstance(connectionConfiguration).setVersion(versioningConfig);
		}
		if (task == Task.EXPORT || task == Task.ALL) {
			DataExportService dataExportService = DataExportService.getInstance(connectionConfiguration);
			List<HierarchicalConfiguration<ImmutableNode>> exportConfigs = configuration
					.configurationsAt(dot(EXPORTS, EXPORT));
			for (HierarchicalConfiguration<ImmutableNode> exportConfig : exportConfigs) {
				dataExportService.exportData(exportConfig);
			}
		}
	}

}
