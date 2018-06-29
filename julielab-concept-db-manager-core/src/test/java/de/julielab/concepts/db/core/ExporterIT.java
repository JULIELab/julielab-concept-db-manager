package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.db.core.services.DataExportService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;
import static org.assertj.core.api.Assertions.assertThat;

@Test(suiteName = "integration-tests", dependsOnGroups = "fruit-concepts")
public class ExporterIT {
    private final static Logger log = LoggerFactory.getLogger(ExporterIT.class);

    public void testExportDictViaPlugin() throws Exception {
        // In this test, we expect that the InserterIT has already been run (see src/test/resources/testsuits/integration-tests.xml).
        // Thus, two concepts should be in the database with a total of three synonyms. So we expect three lines in the output
        // dictionary.
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> confbuilder = new FileBasedConfigurationBuilder<>
                (XMLConfiguration.class).configure(params.xml().setFileName("src/test/resources/exporterconfig.xml"));
        XMLConfiguration conf = confbuilder.getConfiguration();
        conf.setProperty(dot(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        DataExportService exportService = DataExportService.getInstance(conf.configurationAt("connection"));
        exportService.exportData(conf.configurationAt(dot(EXPORTS, EXPORT)));

        try (BufferedReader br = FileUtilities.getReaderFromFile(new File("src/test/resources/output/dict.txt"))) {
            long nonCommentLines = br.lines().filter(l -> !l.startsWith("#")).count();
            assertThat(nonCommentLines).isEqualTo(3);
        }
    }

    public void testBoltExport() throws ConfigurationException, DataExportException, ConceptDatabaseConnectionException, IOException {
        // In this test, we do mostly the same as in the exportDictViaPlugin test but we create a specific Cypher
        // query for it.
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/boltexporterconfig.xml"));
        config.setProperty(dot(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));

        DataExportService exportService = DataExportService.getInstance(config.configurationAt(dot(CONNECTION)));
        exportService.exportData(config.configurationAt(dot(EXPORTS, EXPORT)));


        try (BufferedReader br = FileUtilities.getReaderFromFile(new File("src/test/resources/output/boltexport.txt"))) {
            long nonCommentLines = br.lines().filter(l -> !l.startsWith("#")).count();
            assertThat(nonCommentLines).isEqualTo(2);
        }
    }

}
