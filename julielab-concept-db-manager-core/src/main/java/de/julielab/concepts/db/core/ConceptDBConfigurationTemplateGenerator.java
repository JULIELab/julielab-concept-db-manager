package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.*;
import de.julielab.jssf.commons.spi.ConfigurationTemplateGenerator;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.jssf.commons.util.ConfigurationException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class ConceptDBConfigurationTemplateGenerator implements ConfigurationTemplateGenerator {

    private static ConceptDBConfigurationTemplateGenerator registry;
    private Map<ParameterExposing, String> serviceRegistry;

    public static ConceptDBConfigurationTemplateGenerator getInstance() {
        if (registry == null)
            registry = new ConceptDBConfigurationTemplateGenerator();
        return registry;
    }

    private ConceptDBConfigurationTemplateGenerator() {
        serviceRegistry = new LinkedHashMap<>();
        serviceRegistry.put(VersioningService.getInstance(null), VERSIONING);
        serviceRegistry.put(BoltConnectionService.getInstance(), CONNECTION);
        serviceRegistry.put(ConceptCreationService.getInstance(), slash(IMPORTS, IMPORT));
        serviceRegistry.put(MappingCreationService.getInstance(null), slash(IMPORTS, IMPORT));
        serviceRegistry.put(DatabaseOperationService.getInstance(null), slash(OPERATIONS, OPERATION));
        serviceRegistry.put(DataExportService.getInstance(null), slash(EXPORTS, EXPORT));
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        for (ParameterExposing service : serviceRegistry.keySet()) {
            String path = serviceRegistry.get(service);
            service.exposeParameters(slash(basePath, path), template);
        }
    }

    /**
     * Creates the configuration template. Note that only those components will be reflected in the configuration
     * that have been added to the classpath.
     * @param args
     * @throws ConfigurationException
     */
    public static void main(String args[]) throws ConfigurationException {
        ConceptDBConfigurationTemplateGenerator r = ConceptDBConfigurationTemplateGenerator.getInstance();
        // TODO https://stackoverflow.com/questions/15971497/apache-commons-configuration-setting-an-xml-namespace-for-the-root-element
        r.writeConfigurationTemplate(new File("concept-db-configuration-template.xml"));
    }
}

