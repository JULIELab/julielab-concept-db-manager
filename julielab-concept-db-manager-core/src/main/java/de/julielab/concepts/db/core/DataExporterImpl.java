package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.json.JSONArray;
import org.slf4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

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
    protected String decode(String inputData, HierarchicalConfiguration<ImmutableNode> decodingConfig)
            throws IOException {
        Object currentDataState = inputData;
        for (String key : (Iterable<String>) decodingConfig::getKeys) {
            if (key.equalsIgnoreCase("base64") && decodingConfig.getBoolean(key)) {
                log.debug("Decoding input data via Base64.");
                String currentDataString = toString(currentDataState);
                try {
                    currentDataState = Base64.getDecoder().decode(currentDataString);
                } catch (IllegalArgumentException e) {
                    // There are quotes we don't want
                    if (e.getMessage().contains("Illegal base64 character 22"))
                        currentDataState = Base64.getDecoder().decode(currentDataString.substring(1, currentDataString.length() - 1));
                }
            }
            if (key.equalsIgnoreCase("gzip") && decodingConfig.getBoolean(key)) {
                log.debug("Decoding input data via GZIP.");
                InputStream is = new ByteArrayInputStream(
                        currentDataState instanceof String ? ((String) currentDataState).getBytes()
                                : (byte[]) currentDataState);
                is = new GZIPInputStream(is);
                try (BufferedInputStream bw = new BufferedInputStream(is);
                     ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
                    byte[] buffer = new byte[2048];
                    int numread = 0;
                    while ((numread = bw.read(buffer)) != -1) {
                        baos.write(buffer, 0, numread);
                    }
                    currentDataState = baos.toByteArray();
                }
            }
            if (key.equalsIgnoreCase("json2bytearray") && decodingConfig.getBoolean(key)) {
                log.debug("Decoding input data by converting a JSON array of byte values into a byte array.");
                JSONArray jsonArray = new JSONArray(toString(currentDataState));
                byte[] bytes = new byte[jsonArray.length()];
                for (int i = 0; i < jsonArray.length(); i++) {
                    bytes[i] = (byte) jsonArray.getInt(i);
                }
                currentDataState = bytes;
            }
        }
        return toString(currentDataState);
    }

    protected void writeData(File outputFile, String resourceHeader, String decodedResponse) throws IOException, DataExportException {
        if (!outputFile.getAbsoluteFile().getParentFile().exists())
            outputFile.getAbsoluteFile().getParentFile().mkdirs();
        try (BufferedWriter bw = FileUtilities.getWriterToFile(outputFile)) {
            bw.write(resourceHeader);
            bw.write(decodedResponse);
        }
    }


    private String toString(Object o) {
        if (o instanceof String)
            return (String) o;
        else if (o instanceof byte[]) {
            return new String((byte[]) o, StandardCharsets.UTF_8);
        } else
            throw new IllegalArgumentException("The passed object is neither a string nor a byte[]: " + o);
    }
}
