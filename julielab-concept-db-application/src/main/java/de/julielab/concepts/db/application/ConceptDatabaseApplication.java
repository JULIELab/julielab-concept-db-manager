package de.julielab.concepts.db.application;

import de.julielab.concepts.db.core.services.*;
import de.julielab.concepts.util.*;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang.StringUtils;
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
import static de.julielab.java.utilities.ConfigurationUtilities.slash;


public class ConceptDatabaseApplication {

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


        File configFile = options.configurationFile;
        log.debug("Reading configuration from {}", configFile);
        try {
            XMLConfiguration configuration = ConfigurationUtilities.loadXmlConfiguration(configFile);
            run(options, configuration);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(
                    "The configuration file " + configFile.getAbsolutePath() + " could not be loaded.", e);
        } catch (ConceptCreationException e) {
            log.error("Database creation could not be completed because the concept creation failed.", e);
        } catch (FacetCreationException e) {
            log.error("Database creation could not be completed because the facet creation failed.", e);
        } catch (ConceptInsertionException e) {
            log.error("Concept insertion failed", e);
        } catch (DatabaseOperationException e) {
            log.error("Database operation could not be completed.", e);
        }
    }

    private static void run(CLIParameters parameters, XMLConfiguration configuration) throws ConceptCreationException,
            FacetCreationException, ConceptInsertionException, DataExportException, ConceptDatabaseConnectionException, VersioningException, DatabaseOperationException {
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);

        if (parameters.doImport != null || parameters.doAll != null) {
            ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
            ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);

            if (!emptyNames(parameters.doImport)) {
                for (String importerName : parameters.doImport) {
//                    configuration.configurationAt(slash(IMPORTS, IMPORT+"@y"))
                }
            }
            List<HierarchicalConfiguration<ImmutableNode>> importConfigs = configuration
                    .configurationsAt(slash(IMPORTS, IMPORT));
            for (HierarchicalConfiguration<ImmutableNode> importConfig : importConfigs) {
                Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfig);
                insertionService.insertConcepts(importConfig, concepts);
            }
        }
        if (parameters.doOperation != null || parameters.doAll != null) {
            DatabaseOperationService operationService = DatabaseOperationService.getInstance(connectionConfiguration);
            List<HierarchicalConfiguration<ImmutableNode>> operationConfigs = configuration
                    .configurationsAt(slash(OPERATIONS, OPERATION));
            for (HierarchicalConfiguration<ImmutableNode> operationConfig : operationConfigs) {
                operationService.operate(operationConfig);
            }
        }
        if (parameters.doVersioning || parameters.doAll != null) {
            HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
            VersioningService.getInstance(connectionConfiguration).setVersion(versioningConfig);
        }
        if (parameters.doExport != null || parameters.doAll != null) {
            DataExportService dataExportService = DataExportService.getInstance(connectionConfiguration);
            List<HierarchicalConfiguration<ImmutableNode>> exportConfigs = configuration
                    .configurationsAt(slash(EXPORTS, EXPORT));
            for (HierarchicalConfiguration<ImmutableNode> exportConfig : exportConfigs) {
                dataExportService.exportData(exportConfig);
            }
        }
    }

    private static boolean emptyNames(List<String> selectedStepNames) {
        if (selectedStepNames == null)
            return true;
        return StringUtils.isEmpty(selectedStepNames.get(0));
    }

    /**
     * Convenience method to check if a specific step - i.e. a concept creator, an operation or an exporter - should
     * be performed. Note that <tt>selectedStepNames</tt> is not allowed to be empty. It may be null though, which results
     * into <tt>false</tt> as a return value. To indicate that there are no values,
     * the first element of the list must be the empty string. This is due to the fact that args4j is seemingly not
     * capable of distinguishing between an option given without arguments or an option not given at all. This is why
     * we use the custom option handler {@link OptionalStringArrayOptionHandler} which performs a workaround by placing
     * the empty string into the value list in case that the option is specified but no arguments are given.
     *
     * @param configuration     The XML configuration.
     * @param stepNameKey       The key to get the step name from the configuration.
     * @param selectedStepNames The given step names. If no step names are given, a list is expected whose first element is the empty string.
     * @return If the configured step name should be performed.
     */
    private static boolean stepNameMatches(XMLConfiguration configuration, String stepNameKey, List<String> selectedStepNames) {
        if (selectedStepNames == null)
            return false;
        return StringUtils.isEmpty(selectedStepNames.get(0)) || selectedStepNames.contains(configuration.getString(stepNameKey));
    }

}
