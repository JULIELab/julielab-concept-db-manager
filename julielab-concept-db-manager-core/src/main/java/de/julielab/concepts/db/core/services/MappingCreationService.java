package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.MappingCreator;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingCreationException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.IMPORT;
import static de.julielab.concepts.db.core.ConfigurationConstants.IMPORTS;
import static de.julielab.jssf.commons.Configurations.last;
import static de.julielab.jssf.commons.Configurations.slash;

public class MappingCreationService implements ParameterExposing {
    private final static Logger log = LoggerFactory.getLogger(MappingCreationService.class);
    private static MappingInsertionService service;
    private static Map<HierarchicalConfiguration<ImmutableNode>, MappingCreationService> serviceMap;
    private final ServiceLoader<MappingCreator> loader;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    private MappingCreationService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
        loader = ServiceLoader.load(MappingCreator.class);
    }

    /**
     * Returns the mapping creation service singleton specifically created for this
     * passed configuration.
     *
     * @param connectionConfiguration
     * @return
     */
    public static synchronized MappingCreationService getInstance(
            HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        if (serviceMap == null)
            serviceMap = new HashMap<>();
        return serviceMap.computeIfAbsent(connectionConfiguration, MappingCreationService::new);
    }

    public Stream<ImportMapping> createMappings(HierarchicalConfiguration<ImmutableNode> importConfig) throws MappingCreationException {
        Iterator<MappingCreator> creatorIt = loader.iterator();
        while (creatorIt.hasNext()) {
            MappingCreator creator = creatorIt.next();
            try {
                creator.setConnection(connectionConfiguration);
                return creator.createMappings(importConfig);
            } catch (ConceptDatabaseConnectionException e) {
                log.debug("Mapping Creator {} is omitted because it does not accept the given connection: {}", creator.getClass().getCanonicalName(), e.getMessage());
            }
        }
        throw new MappingCreationException(
                "Mapping creation failed because no mapping creator for the import and connection configuration "
                        + ConfigurationUtils.toString(importConfig) + ", "
                        + ConfigurationUtils.toString(connectionConfiguration)
                        + " was found. Make sure that an appropriate connection provider is given in the META-INF/services/"
                        + MappingInserter.class.getCanonicalName() + " file.");
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        Iterator<MappingCreator> creatorIt = loader.iterator();
        while (creatorIt.hasNext()) {
            template.addProperty(basePath, "");
            MappingCreator creator = creatorIt.next();
            creator.exposeParameters(last(basePath), template);
        }
    }
}
