package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.ConceptCreationService;
import de.julielab.concepts.db.core.services.DataExportService;
import de.julielab.concepts.db.core.services.DatabaseOperationService;
import de.julielab.jssf.commons.spi.ConfigurationTemplateGenerator;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.jssf.commons.util.ConfigurationException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.io.FileBased;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.jssf.commons.Configurations.slash;

public class ConceptDBServiceRegistry implements ConfigurationTemplateGenerator {

    private static ConceptDBServiceRegistry registry;
    private Map<ParameterExposing, String> serviceRegistry;

    public static ConceptDBServiceRegistry getInstance() {
        if (registry == null)
            registry = new ConceptDBServiceRegistry();
        return registry;
    }

    private ConceptDBServiceRegistry() {
        serviceRegistry = new LinkedHashMap<>();
//        serviceRegistry.put(VersioningService.class, VERSIONING);
        serviceRegistry.put(BoltConnectionService.getInstance(), CONNECTION);
        serviceRegistry.put(ConceptCreationService.getInstance(), slash(IMPORTS, IMPORT));
        serviceRegistry.put(DatabaseOperationService.getInstance(null), slash(OPERATIONS, OPERATION));
        serviceRegistry.put(DataExportService.getInstance(null), slash(EXPORTS, EXPORT));
//        serviceRegistry.put(FacetCreationService.class, slash(IMPORTS, IMPORT, FACET, CREATOR));
//        serviceRegistry.put(FileConnectionService.class, CONNECTION);
//        serviceRegistry.put(HttpConnectionService.class, CONNECTION);
//        serviceRegistry.put(MappingInsertionService.class, slash(IMPORTS, IMPORT));
    }

    @Override
    public HierarchicalConfiguration<ImmutableNode> createConfigurationTemplate() throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder =
                new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(params.xml()
                                .setExpressionEngine(new XPathExpressionEngine())
                                .setEncoding(StandardCharsets.UTF_8.name())
                        );
        XMLConfiguration c;
        try {
            c = builder.getConfiguration();
            exposeParameters("", c);
        } catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
            throw new ConfigurationException();
        }
        return c;
    }

    @Override
    public void writeConfigurationTemplate(File destination) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> template = createConfigurationTemplate();
            if (!(template instanceof FileBased))
                throw new ConfigurationException("The created configuration cannot be stored to file " +
                        "because the chosen configuration implementation " + template.getClass().getCanonicalName() + " " +
                        "does not implement the " + FileBased.class.getCanonicalName() + " interface");
            FileHandler fh = new FileHandler((FileBased) template);
            System.out.println( ConfigurationUtils.toString(template));




            fh.save(destination);
        } catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
            throw new ConfigurationException();
        }
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        for (ParameterExposing service : serviceRegistry.keySet()) {
            String path = serviceRegistry.get(service);
            service.exposeParameters(path, template);
        }
    }

}

