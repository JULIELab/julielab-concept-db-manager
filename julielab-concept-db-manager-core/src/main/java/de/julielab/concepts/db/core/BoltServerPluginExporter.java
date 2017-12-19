package de.julielab.concepts.db.core;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.driver.v1.Driver;

import de.julielab.concepts.db.core.services.BoltConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;

public class BoltServerPluginExporter extends DataExporterBase {

	private static final String CONFKEY_PLUGIN_ENDPOINT = "configuration.pluginendpoint";
	private static final String CONFKEY_PARAMETERS = "configuration.parameters";

	private Driver driver;

	@Override
	public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) {
		String pluginEndpoint = exportConfig.getString(CONFKEY_PLUGIN_ENDPOINT);
		HierarchicalConfiguration<ImmutableNode> parameterConfiguration = exportConfig
				.configurationAt(CONFKEY_PARAMETERS);
		Map<String, Parameter> parameterMap = parseParameters(parameterConfiguration);
		
	}

	



	@Override
	public boolean hasName(String providerName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		try {
			driver = BoltConnectionService.getInstance().getBoltDriver(connectionConfiguration);
		} catch (IOException e) {
			throw new ConceptDatabaseConnectionException(e);
		}
	}

}
