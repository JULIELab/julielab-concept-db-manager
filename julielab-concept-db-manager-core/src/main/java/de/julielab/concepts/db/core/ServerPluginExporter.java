package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.MethodCallException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.*;

public class ServerPluginExporter extends ServerPluginCallBase implements DataExporter {

    public static final String CONFKEY_DECODING = "configuration.decoding";
    public static final String CONFKEY_OUTPUT_FILE = "configuration.outputfile";

    private static final Logger log = LoggerFactory.getLogger(ServerPluginExporter.class);

    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

    public ServerPluginExporter() {
        super(log);
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
            throws ConceptDatabaseConnectionException, DataExportException {
        String baseUri = connectionConfiguration.getString(NetworkConnectionCredentials.CONFKEY_URI);
        String pluginName = exportConfig.getString(CONFKEY_PLUGIN_NAME);
        String pluginEndpoint = exportConfig.getString(CONFKEY_PLUGIN_ENDPOINT);
        File outputFile = new File(exportConfig.getString(CONFKEY_OUTPUT_FILE));
        String response = null;
        try {
            response = callNeo4jServerPlugin(connectionConfiguration, exportConfig);
            log.info("Writing file {}", outputFile);
            String decodedResponse = decode(response, exportConfig.configurationAt(CONFKEY_DECODING));
            writeData(outputFile, decodedResponse);
            log.info("Done.");
        } catch (UnsupportedEncodingException e) {
            throw new ConceptDatabaseConnectionException(e);
        } catch (IOException e) {
            throw new DataExportException("Decoding the retrieved data failed. Decoding configuration is "
                    + ConfigurationUtils.toString(exportConfig.configurationAt(CONFKEY_DECODING)), e);
        } catch (JSONException e) {
            log.error("Converting the retrieved data into a JSON structure failed. The data was {}", response, e);
        } catch (MethodCallException e) {
            throw new DataExportException(e);
        }
    }

    public void writeData(File outputFile, String decodedResponse) throws IOException, DataExportException {
        if (!outputFile.getParentFile().exists())
            outputFile.getParentFile().mkdirs();
        try (BufferedWriter bw = FileUtilities.getWriterToFile(outputFile)) {
            bw.write(getResourceHeader(connectionConfiguration));
            bw.write(decodedResponse);
        } catch (VersionRetrievalException e) {
            throw new DataExportException("Exception when retrieving database version", e);
        }
    }

    @Override
    public boolean hasName(String providerName) {
        return providerName.equalsIgnoreCase("serverpluginexporter")
                || providerName.equals(getClass().getCanonicalName());
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException {
        try {
            HttpConnectionService httpService = HttpConnectionService.getInstance();
            // Check if there will be an error thrown due to an invalid URI or something.
            httpService.getHttpPostRequest(connectionConfiguration);
            this.connectionConfiguration = connectionConfiguration;
        } catch (ConceptDatabaseConnectionException e) {
            throw new ConceptDatabaseConnectionException(e);
        }
    }

}
