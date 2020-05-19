package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.DataExportService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;

@Test(suiteName = "integration-tests", dependsOnGroups = "fruit-concepts")
public class ExporterIT {
    private final static Logger log = LoggerFactory.getLogger(ExporterIT.class);

    public void testExportDictViaPlugin() throws Exception {
        // In this test, we expect that the InserterIT has already been run (see src/test/resources/testsuits/integration-tests.xml).
        // Thus, two concepts should be in the database with a total of three synonyms. So we expect three lines in the output
        // dictionary.
        XMLConfiguration conf = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/serverpluginexportconfig.xml"));
        conf.setProperty(slash(CONNECTION, URI), "http://localhost:" + ITTestsSetup.neo4j.getMappedPort(7474));
        DataExportService exportService = DataExportService.getInstance(conf.configurationAt("connection"));
        exportService.exportData(conf.configurationAt(slash(EXPORTS, EXPORT)));

        try (BufferedReader br = FileUtilities.getReaderFromFile(new File("src/test/resources/output/dict.txt"))) {
            long nonCommentLines = br.lines().filter(l -> !l.startsWith("#")).count();
            assertThat(nonCommentLines).isEqualTo(3);
        }
    }

    public void testBoltExport() throws ConfigurationException, DataExportException, ConceptDatabaseConnectionException, IOException {
        // In this test, we do mostly the same as in the exportDictViaPlugin test but we create a specific Cypher
        // query for it.
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/boltexporterconfig.xml"));
        config.setProperty(slash(CONNECTION, URI), "bolt://localhost:" + ITTestsSetup.neo4j.getMappedPort(7687));

        DataExportService exportService = DataExportService.getInstance(config.configurationAt(slash(CONNECTION)));
        exportService.exportData(config.configurationAt(slash(EXPORTS, EXPORT)));


        try (BufferedReader br = FileUtilities.getReaderFromFile(new File("src/test/resources/output/boltexport.txt"))) {
            long nonCommentLines = br.lines().filter(l -> !l.startsWith("#")).count();
            assertThat(nonCommentLines).isEqualTo(2);
        }
    }

}
