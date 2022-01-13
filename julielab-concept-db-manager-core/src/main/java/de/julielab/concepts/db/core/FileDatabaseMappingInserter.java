package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.MappingInserter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.neo4j.plugins.concepts.ConceptInsertion;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.FormattedLogFormat.PLAIN;

public class FileDatabaseMappingInserter extends JavaMethodCallBase implements MappingInserter {

    private final static Logger log = LoggerFactory.getLogger(FileDatabaseMappingInserter.class);
    private final Log ciLog;
    private DatabaseManagementService dbms;

    public FileDatabaseMappingInserter() {
        super(log);
        Log4jLogProvider log4jLogProvider = new Log4jLogProvider(LogConfig.createBuilder(System.out, Level.INFO)
                .withFormat(PLAIN)
                .withCategory(false)
                .build());
        ciLog = log4jLogProvider.getLog(ConceptInsertion.class);
    }

    @Override
    public void insertMappings(HierarchicalConfiguration<ImmutableNode> importConfiguration, Stream<ImportMapping> mappings) throws MappingInsertionException {
        if (dbms == null)
            throw new MappingInsertionException(
                    "No access to a file-based graph database. " +
                            "The FileDatabaseMappingInserter has not been initialized properly. " +
                            "Call setConfiguration() and check for thrown exceptions before calling this method.");
        if (log.isInfoEnabled())
            log.info("Inserting mappings into file based Neo4j database");
        try (Transaction tx = dbms.database(DEFAULT_DATABASE_NAME).beginTx()) {
            ConceptInsertion.insertMappings(tx, ciLog, mappings.iterator());
        }

    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException {
        dbms = FileConnectionService.getInstance().getDatabaseManagementService(connectionConfiguration);
        if (dbms == null)
            throw new ConceptDatabaseConnectionException("Could not create a file database for connection "
                    + ConfigurationUtils.toString(connectionConfiguration));
    }
}
