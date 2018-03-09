package de.julielab.concepts.db.core.services;

import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.concepts.util.MappingInsertionException;
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

public class MappingInsertionService {

    private final static Logger log = LoggerFactory.getLogger(MappingInsertionService.class);

    private final ServiceLoader<MappingInserter> loader;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
    private static MappingInsertionService service;
    private static Map<HierarchicalConfiguration<ImmutableNode>, MappingInsertionService> serviceMap;

    private MappingInsertionService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
        loader = ServiceLoader.load(MappingInserter.class);
    }

    /**
     * Returns the data export service singleton specifically created for this
     * passed configuration.
     *
     * @param connectionConfiguration
     * @return
     */
    public static synchronized MappingInsertionService getInstance(
            HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
        if (serviceMap == null)
            serviceMap = new HashMap<>();
        return serviceMap.computeIfAbsent(connectionConfiguration, MappingInsertionService::new);
    }

    public void insertMappings(Stream<ImportMapping> mappings) throws MappingInsertionException {
        Iterator<MappingInserter> inserterIt = loader.iterator();
        boolean inserterFound = false;
        while (inserterIt.hasNext()) {
            MappingInserter inserter = inserterIt.next();
            try {
                inserter.setConnection(connectionConfiguration);
                inserter.insertMappings(mappings);
                inserterFound = true;
            } catch (ConceptDatabaseConnectionException e) {
                log.debug("Mapping inserter " + inserter.getClass().getCanonicalName() + " could not serve the connection configuration " + ConfigurationUtils.toString(connectionConfiguration) + ": " + e.getMessage());
            }
        }
        if (!inserterFound)
            throw new MappingInsertionException(
                    "Mapping insertion failed because no mapping inserter for the connection configuration "
                            + ConfigurationUtils.toString(connectionConfiguration)
                            + " was found. Make sure that an appropriate connection provider is given in the META-INF/services/"
                            + MappingInserter.class.getCanonicalName() + " file.");
    }
}
