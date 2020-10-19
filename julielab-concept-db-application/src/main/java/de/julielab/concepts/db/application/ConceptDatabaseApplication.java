package de.julielab.concepts.db.application;

import de.julielab.concepts.db.core.services.*;
import de.julielab.concepts.util.*;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerRegistry;
import org.kohsuke.args4j.ParserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
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

        if (parameters.doPreparation != null || parameters.doAll != null) {
            doOperations(parameters, configuration, connectionConfiguration, PREPARATIONS);
        }
        if (parameters.doImport != null || parameters.doAll != null) {
            doImports(parameters, configuration, connectionConfiguration);
        }
        if (parameters.doOperation != null || parameters.doAll != null) {
            doOperations(parameters, configuration, connectionConfiguration, OPERATIONS);
        }
        if (!parameters.noVersioning && (parameters.doVersioning || parameters.doAll != null)) {
            HierarchicalConfiguration<ImmutableNode> versioningConfig = configuration.configurationAt(VERSIONING);
            VersioningService.getInstance(connectionConfiguration).setVersion(versioningConfig);
        }
        if (parameters.doExport != null || parameters.doAll != null) {
            doExports(parameters, configuration, connectionConfiguration);
        }
    }

    private static void doExports(CLIParameters parameters, XMLConfiguration configuration, HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws DataExportException, ConceptDatabaseConnectionException {
        DataExportService dataExportService = DataExportService.getInstance(connectionConfiguration);
        List<HierarchicalConfiguration<ImmutableNode>> applicableExports = new ArrayList<>();
        List<String> selectedSteps = !isUnspecified(parameters.doExport) ? parameters.doExport : parameters.doAll;

        if (!isUnspecified(selectedSteps)) {
            for (String exportName : selectedSteps) {
                try {
                    HierarchicalConfiguration<ImmutableNode> exportConfig = configuration.configurationAt(slash(EXPORTS, EXPORT + "[@name='" + exportName + "']"));
                    applicableExports.add(exportConfig);
                } catch (ConfigurationRuntimeException e) {
                    // When doAll is active, we do not print the warning because it would activate all the time
                    // without use
                    if (!isUnspecified(parameters.doExport)) {
                        log.warn("There is no export with name {} in the configuration file.", exportName);
                        log.trace("Exception was: ", e);
                    }
                }
            }
        } else {
            applicableExports = configuration
                    .configurationsAt(slash(EXPORTS, EXPORT));
        }
        for (HierarchicalConfiguration<ImmutableNode> exportConfig : applicableExports) {
            dataExportService.exportData(exportConfig);
        }
    }

    private static void doOperations(CLIParameters parameters, XMLConfiguration configuration, HierarchicalConfiguration<ImmutableNode> connectionConfiguration, String baseElementName) throws DatabaseOperationException {
        DatabaseOperationService operationService = DatabaseOperationService.getInstance(connectionConfiguration);
        List<HierarchicalConfiguration<ImmutableNode>> applicableOperations = new ArrayList<>();

        List<String> selectedSteps = !isUnspecified(parameters.doOperation) ? parameters.doOperation : parameters.doAll;

        if (!isUnspecified(selectedSteps)) {
            for (String importerName : selectedSteps) {
                try {
                    HierarchicalConfiguration<ImmutableNode> operationConfig = configuration.configurationAt(slash(baseElementName, OPERATION + "[@name='" + importerName + "']"));
                    applicableOperations.add(operationConfig);
                } catch (ConfigurationRuntimeException e) {
                    // When doAll is active, we do not print the warning because it would activate all the time
                    // without use
                    if (!isUnspecified(parameters.doOperation)) {
                        log.warn("There is no operation with name {} in the configuration file.", importerName);
                        log.trace("Exception was: ", e);
                    }
                }
            }
        } else {
            applicableOperations = configuration
                    .configurationsAt(slash(baseElementName, OPERATION));
        }
        for (HierarchicalConfiguration<ImmutableNode> operationConfig : applicableOperations) {
            operationService.operate(operationConfig);
        }
    }

    private static void doImports(CLIParameters parameters, XMLConfiguration configuration, HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptCreationException, FacetCreationException, ConceptInsertionException {
        ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);
        List<HierarchicalConfiguration<ImmutableNode>> applicableImports = new ArrayList<>();

        List<String> selectedSteps = !isUnspecified(parameters.doImport) ? parameters.doImport : parameters.doAll;

        if (!isUnspecified(selectedSteps)) {
            for (String importerName : selectedSteps) {
                try {
                    HierarchicalConfiguration<ImmutableNode> importConfig = configuration.configurationAt(slash(IMPORTS, IMPORT + "[@name='" + importerName + "']"));
                    applicableImports.add(importConfig);
                } catch (ConfigurationRuntimeException e) {
                    // When doAll is active, we do not print the warning because it would activate all the time
                    // without use
                    if (!isUnspecified(parameters.doImport)) {
                        log.warn("There is no import with name {} in the configuration file.", importerName);
                        log.trace("Exception was: ", e);
                    }
                }
            }
        } else {
            applicableImports = configuration
                    .configurationsAt(slash(IMPORTS, IMPORT));
        }
        for (HierarchicalConfiguration<ImmutableNode> importConfig : applicableImports) {
            Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfig);
            insertionService.insertConcepts(importConfig, concepts);
        }
    }

    /**
     * Checks if there are names of imports, operations or exports specified. If so, only those steps will be
     * performed. Note that <tt>selectedStepNames</tt> is not allowed to be empty. It may be null though, which results
     * into <tt>true</tt> as a return value. To indicate that there are no values,
     * the first element of the list must be the empty string. This is due to the fact that args4j is seemingly not
     * capable of distinguishing between an option given without arguments or an option not given at all. This is why
     * we use the custom option handler {@link OptionalStringArrayOptionHandler} which performs a workaround by placing
     * the empty string into the value list in case that the option is specified but no arguments are given.
     *
     * @param selectedStepNames
     * @return
     */
    private static boolean isUnspecified(List<String> selectedStepNames) {
        if (selectedStepNames == null)
            return true;
        return StringUtils.isEmpty(selectedStepNames.get(0));
    }
}
