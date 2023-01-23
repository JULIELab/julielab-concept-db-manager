package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.concepts.ConceptInsertion;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.FormattedLogFormat.PLAIN;

public class FileDatabaseConceptInserter implements ConceptInserter {

    private static final Logger log = LoggerFactory.getLogger(FileDatabaseConceptInserter.class);
    private final Log ciLog;
    private DatabaseManagementService dbms;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public FileDatabaseConceptInserter() {
        Log4jLogProvider log4jLogProvider = new Log4jLogProvider(LogConfig.createBuilder(System.out, Level.INFO)
                .withFormat(PLAIN)
                .withCategory(false)
                .build());
        ciLog = log4jLogProvider.getLog(ConceptInsertion.class);
    }

    @Override
    public void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfiguration, ImportConcepts concepts) throws ConceptInsertionException {
        if (dbms == null)
            throw new ConceptInsertionException(
                    "No access to a file-based graph database. The FileDatabaseConceptInserter has not been initialized properly. Call setConfiguration() and check its return value before calling this method.");
        log.info("Inserting concepts into embedded Neo4j database at {}.", connectionConfiguration.getString("uri"));

        concepts.setImportOptions(setGlobalOptions(importConfiguration, concepts.getImportOptions()));

        ImportFacet facet = concepts.getFacet();
        if (facet == null)
            throw new ConceptInsertionException("The facet of the import concepts is null.");
        String customId = facet.getCustomId();
        boolean alreadyExists;
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            Node facetNode = tx.findNode(FacetLabel.FACET, FacetConstants.PROP_CUSTOM_ID, customId);
            alreadyExists = facetNode != null;
        }
        if (!alreadyExists) {
            try {
                log.debug("Inserting the concepts of facet {} (customId: {}) into the Neo4j database", facet.getName(),
                        facet.getCustomId());
                // The JULIE Lab Neo4j plugins cannot handle parents in merging mode
                if (concepts.getImportOptions().merge)
                    concepts.setConcepts(concepts.getConcepts().map(c -> {c.parentCoordinates = Collections.emptyList(); return c;}));
                Map<String, Object> response = new HashMap<>();
                ConceptInsertion.insertConcepts(graphDb, ciLog, concepts, response);
                log.debug("Successfully inserted the given concepts: {}", response);
            } catch (de.julielab.neo4j.plugins.util.ConceptInsertionException e) {
                throw new ConceptInsertionException(
                        "The JSON format specifying the ontology class names or - but less probable - the facet JSON format does not fit the requirements of the employed version of the julielab-neo4j-plugin-concepts dependency. There might be a compatibility issue between the julielab-bioportal-tools and the plugin-concepts libraries.",
                        e);
            }
        } else {
            // ontology facet node was found
            log.debug("Facet with custom ID {} already exists in the database and is not inserted again.",
                    facet.getCustomId());
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        dbms = FileConnectionService.getInstance().getDatabaseManagementService(connectionConfiguration);
        if (dbms == null)
            throw new ConceptDatabaseConnectionException("Could not create a file database for connection "
                    + ConfigurationUtils.toString(connectionConfiguration));
    }

    public enum VersionLabel implements Label {
        VERSION
    }

}
