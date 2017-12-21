import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;

import de.julielab.concepts.db.core.services.DataExportService;

public class ExporterTest {

	public static void main(String[] args) throws Exception {
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> confbuilder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class).configure(params.xml().setFileName("src/test/resources/exporterconfig.xml"));
		XMLConfiguration conf = confbuilder.getConfiguration();
		DataExportService exportService = DataExportService.getInstance(conf.configurationAt("connection"));
		exportService.exportData(conf.configurationAt("exports.export"));
	}

}
