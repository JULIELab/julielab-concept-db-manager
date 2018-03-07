package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.DataExportService;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;

public class ExporterIT {
private final static Logger log = LoggerFactory.getLogger(ExporterIT.class);

    @Test(groups = "integration-tests")
    public void testExport() throws Exception {

        Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> confbuilder = new FileBasedConfigurationBuilder<>
				(XMLConfiguration.class).configure(params.xml().setFileName("src/test/resources/exporterconfig.xml"));
		XMLConfiguration conf = confbuilder.getConfiguration();
		conf.setProperty(dot(CONNECTION, URI), "http://localhost:" + TestSuite.neo4j.getMappedPort(7474));
		DataExportService exportService = DataExportService.getInstance(conf.configurationAt("connection"));
		exportService.exportData(conf.configurationAt(dot(EXPORTS, EXPORT)));
    }

}
