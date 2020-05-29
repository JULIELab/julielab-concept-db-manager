package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.MethodCallException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.File;
import java.io.IOException;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class RestExporter extends RestCallBase implements DataExporter  {

    private static final Logger log = LoggerFactory.getLogger(RestExporter.class);

    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
    private final DataExporter exporter;

    public RestExporter() {
        super(log);
        exporter = new DataExporterImpl(log) {
            @Override
            public void exposeParameters(String s, HierarchicalConfiguration<ImmutableNode> c) {
                // not used
            }

            @Override
            public String getName() {
                // not used
                return null;
            }

            @Override
            public void setConnection(HierarchicalConfiguration<ImmutableNode> c){
                // not used
            }

            @Override
            public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws ConceptDatabaseConnectionException, DataExportException {
                String response = null;
                try {
                    String outputPath = ConfigurationUtilities.requirePresent(slash(CONFIGURATION, OUTPUT_FILE), exportConfig::getString);
                    File outputFile = new File(outputPath);
                    response = callNeo4jServerPlugin(connectionConfiguration, exportConfig, "GET");
                    log.info("Writing file {}", outputFile);
                    String decodedResponse = decode(response, exportConfig.configurationAt(slash(CONFIGURATION, DECODING)));
                    writeData(outputFile, getResourceHeader(connectionConfiguration),decodedResponse);
                    log.info("Done.");
                }  catch (IOException e) {
                    throw new DataExportException("Decoding the retrieved data failed. Decoding configuration is "
                            + ConfigurationUtils.toString(exportConfig.configurationAt(slash(CONFIGURATION, DECODING))), e);
                } catch (JSONException e) {
                    log.error("Converting the retrieved data into a JSON structure failed. The data was {}", response, e);
                } catch (MethodCallException e) {
                    throw new DataExportException(e);
                } catch (ConfigurationException e) {
                    log.error("Configuration problem with export configuration {}", ConfigurationUtils.toString(exportConfig));
                    throw new DataExportException(e);
                } catch (VersionRetrievalException e) {
                    throw new DataExportException(e);
                }
            }
        };
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
            throws ConceptDatabaseConnectionException, DataExportException {
       exporter.exportData(exportConfig);
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        HttpConnectionService httpService = HttpConnectionService.getInstance();
        // Check if there will be an error thrown due to an invalid URI or something.
        httpService.getHttpRequest(connectionConfiguration, HttpMethod.GET);
        this.connectionConfiguration = connectionConfiguration;
    }

    @Override
    public String getName() {
        return "ServerPluginExporter";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        super.exposeParameters(basePath, template);
        template.addProperty(slash(basePath, CONFIGURATION, DECODING, JSON2BYTEARRAY), "false");
        template.addProperty(slash(basePath, CONFIGURATION, DECODING, BASE64), "true");
        template.addProperty(slash(basePath, CONFIGURATION, DECODING, GZIP), "true");
        template.addProperty(slash(basePath, CONFIGURATION, OUTPUT_FILE), "");
    }

}