package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.*;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class JavaClassFileDBExporter extends JavaMethodCallBase implements DataExporter {

    private final static Logger log = LoggerFactory.getLogger(JavaClassFileDBExporter.class);


    private DatabaseManagementService dbms;
    private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
    private final DataExporter exporter;

    public JavaClassFileDBExporter() {
        super(log);
        // Since this class needs to extend JavaMethodCallBase, we do the composition pattern here.
        // This issue might be a good hint towards "Composition over inheritance" anyway...
        exporter = new DataExporterImpl(log) {
            @Override
            public void exposeParameters(String s, HierarchicalConfiguration<ImmutableNode> hierarchicalConfiguration) {
                // not used
            }

            @Override
            public String getName() {
                // not used
                return null;
            }

            @Override
            public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
                dbms = FileConnectionService.getInstance().getDatabaseManagementService(connectionConfiguration);
            }

            @Override
            public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig) throws DataExportException {
                String outputFile = exportConfig.getString(slash(REQUEST, OUTPUT_FILE));
                try {
                    String result = callInstanceMethod(exportConfig.configurationAt(REQUEST), dbms);
                    String decodedResponse = decode(result, exportConfig.configurationAt(slash(REQUEST, DECODING)));
                    String resourceHeader = getResourceHeader(connectionConfiguration) + result;
                    writeData(new File(outputFile), resourceHeader, decodedResponse);
                } catch (MethodCallException | VersionRetrievalException | IOException | JSONException e) {
                    throw new DataExportException(e);
                }
            }
        };
    }

    @Override
    public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
            throws DataExportException, IncompatibleActionHandlerConnectionException {
        try {
            exporter.exportData(exportConfig);
        } catch (ConceptDatabaseConnectionException e) {
            // This exception is not thrown from the internal exporter implementation
            e.printStackTrace();
        }
    }


    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException {
        this.connectionConfiguration = connectionConfiguration;
        exporter.setConnection(connectionConfiguration);
    }

    @Override
    public String getName() {
        return "JavaClassFileDBExporter";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        super.exposeParameters(basePath, template);
        template.addProperty(slash(basePath, REQUEST, DECODING, JSON2BYTEARRAY), "false");
        template.addProperty(slash(basePath, REQUEST, DECODING, BASE64), "true");
        template.addProperty(slash(basePath, REQUEST, DECODING, GZIP), "true");
        template.addProperty(slash(basePath, REQUEST, OUTPUT_FILE), "");
    }
}
