package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;

import java.io.*;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class DataExporterImpl implements DataExporter {

    private Logger log;

    public DataExporterImpl(Logger log) {
        this.log = log;
    }

    /**
     * Creates a small header to be applied to exported resource files. The header contains the database
     * and Concept Manager application versions. Thus should help to avoid confusion about versioning of resources.
     * The lines are prepended with the '#' character. Thus, reading applications should accept this character
     * as a comment indicator.
     *
     * @param connectionConfiguration Connection configuration to retrieve the database version.
     * @return A string representing the header.
     * @throws VersionRetrievalException If the database version cannot be retrieved.
     * @throws IOException               If the application version cannot be read (it is stored in a auto-generated file).
     */
    protected String getResourceHeader(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws VersionRetrievalException, IOException {
        String version = VersioningService.getInstance(connectionConfiguration).getVersion();
        StringBuilder sb = new StringBuilder();
        sb.append("# ").append("Concept database version: ").append(version)
                .append(System.getProperty("line.separator"));
        sb.append("# ").append("Concept Database Manager Application version: ").append(getApplicationVersion())
                .append(System.getProperty("line.separator"));
        return sb.toString();
    }

    private String getApplicationVersion() throws IOException {
        BufferedReader br = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/concept-db-manager-version.txt")));
        return br.readLine().trim();
    }

    /**
     * Decodes data that has been encoded into a string. The <tt>decodingConfig</tt> determines
     * which decodings are applied in which order. The following decodings are available:
     * <dl>
     *     <dt>base64</dt>
     *     <dd>Base64 is an algorithm to encode arbitrary byte sequences into strings.</dd>
     *     <dt>gzip</dt>
     *     <dd>If the data contains a byte array that has been compressed with GZIP and then encoded into a string
     *     using base64, this decoding decompresses the byte array. Requires the base64 encoding to be applied
     *     before.</dd>
     *     <dt>json2bytearray</dt>
     *     <dd>While less performant than using base64, a byte array may also be encoded as a JSON array carrying the
     *     byte values.</dd>
     * </dl>
     *
     * @param inputData
     * @param decodingConfig
     * @return
     * @throws IOException
     */
    protected InputStream decode(InputStream inputData, HierarchicalConfiguration<ImmutableNode> decodingConfig)
            throws IOException {
        InputStream currentDataState = inputData;
        for (String key : (Iterable<String>) decodingConfig::getKeys) {
            if (key.equalsIgnoreCase("base64") && decodingConfig.getBoolean(key)) {
                log.debug("Decoding input data via Base64.");
                currentDataState = Base64.getDecoder().wrap(currentDataState);
            }
            if (key.equalsIgnoreCase("gzip") && decodingConfig.getBoolean(key)) {
                log.debug("Decoding input data via GZIP.");
                currentDataState = new GZIPInputStream(currentDataState);
            }
            if (key.equalsIgnoreCase("json2bytearray") && decodingConfig.getBoolean(key)) {
                throw new IllegalArgumentException("The json2bytearray decoding option is not supported any more.");
            }
        }
        return currentDataState;
    }

    protected void writeData(File outputFile, String resourceHeader, InputStream decodedResponse) throws IOException {
        if (!outputFile.getAbsoluteFile().getParentFile().exists())
            outputFile.getAbsoluteFile().getParentFile().mkdirs();
        try (OutputStream os = FileUtilities.getOutputStreamToFile(outputFile)) {
            os.write(resourceHeader.getBytes(UTF_8));
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = decodedResponse.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }


    private String toString(Object o) {
        if (o instanceof String)
            return (String) o;
        else if (o instanceof byte[]) {
            return new String((byte[]) o, UTF_8);
        } else
            throw new IllegalArgumentException("The passed object is neither a string nor a byte[]: " + o);
    }
}
