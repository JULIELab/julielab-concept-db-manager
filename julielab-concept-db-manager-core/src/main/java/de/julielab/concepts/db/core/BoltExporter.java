package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.CONFIGURATION;
import static de.julielab.concepts.db.core.ConfigurationConstants.CYPHER_QUERY;
import static de.julielab.concepts.db.core.ConfigurationConstants.OUTPUT_FILE;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

/**
 * Sends a given Cypher query and writes the retrieved results into the given output file. One record per line,
 * fields are tab-separated, all field values are converted to strings.
 */
public class BoltExporter implements DataExporter {
    private final static Logger log = LoggerFactory.getLogger(BoltExporter.class);

    private Driver driver;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws ConceptDatabaseConnectionException, DataExportException {
        try {
            String query = ConfigurationUtilities.requirePresent(dot(CONFIGURATION, CYPHER_QUERY), exportConfig::getString);
            File outputFile = new File(ConfigurationUtilities.<String>requirePresent(dot(CONFIGURATION, OUTPUT_FILE), exportConfig::getString));

            log.info("Sending Cypher query {} to Neo4j and writing the results to {}", query, outputFile);
            try (Session session = driver.session(); BufferedWriter bw = FileUtilities.getWriterToFile(outputFile)) {
                bw.write(getResourceHeader(connectionConfiguration));
                StatementResult result = session.readTransaction(tx -> tx.run(query));
                List<String> fieldValues = new ArrayList<>();
                while (result.hasNext()) {
                    Record record = result.next();
                    fieldValues.clear();
                    for (int i = 0; i < record.size(); i++) {
                        Value value = record.get(i);
                        String valueAsString;
                        if (value.hasType(TYPE_SYSTEM.NUMBER()))
                            valueAsString = value.asNumber().toString();
                        else if (value.hasType(TYPE_SYSTEM.NULL()))
                            valueAsString = "";
                        else if (value.hasType(TYPE_SYSTEM.STRING()))
                            valueAsString = value.asString();
                        else if (value.hasType(TYPE_SYSTEM.NODE()))
                            valueAsString = value.asNode().asMap().entrySet().stream().map(e -> e.getKey() + ": " + e.getValue().toString()).collect(Collectors.joining(", "));
                        else
                            throw new DataExportException("The query \"" + query + "\" returned a value of type " + value.type().name() + " which is currently not supported for output.");
                        fieldValues.add(valueAsString);
                    }

                    bw.write(fieldValues.stream().collect(Collectors.joining("\t")));
                    bw.newLine();
                }
            } catch (IOException e) {
                throw new DataExportException(e);
            } catch (VersionRetrievalException e) {
                throw new ConceptDatabaseConnectionException(e);
            }
            log.info("Done.");
        } catch (ConfigurationException e) {
            throw new DataExportException(e);
        }
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        try {
            driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
        } catch (IOException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

    @Override
    public String getName() {
        return "BoltExporter";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, CONFIGURATION, CYPHER_QUERY), "");
        template.addProperty(slash(basePath, CONFIGURATION, OUTPUT_FILE), "");
    }
}
