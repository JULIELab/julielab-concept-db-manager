package de.julielab.concepts.db.core.spi;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.services.VersioningService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.VersionRetrievalException;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;

import javax.xml.bind.DatatypeConverter;

/**
 * <p>
 * Data exporters read a database, extract specific information and store it at
 * some external location in a specific output format.
 * </p>
 * <p>
 * Some complexity arises concerning the connection to the database. Ideally,
 * each data exporter can handle all database connections. The core library
 * delivers connections via BOLT or via direct file access to the database. If a
 * connection can not be obtained, a meaningful exception should be thrown from
 * {@link #setConnection(HierarchicalConfiguration)};
 * </p>
 * 
 * @author faessler
 *
 */
public interface DataExporter extends ExtensionPoint, DatabaseConnected {
	/**
	 * Export data from the database to an external location.
	 * 
	 * @param exportConfig
	 *            Export subconfiguration.
	 * @throws ConceptDatabaseConnectionException
	 * @throws DataExportException
	 */
	void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
			throws ConceptDatabaseConnectionException, DataExportException;

	/**
	 * Creates a small header to be applied to exported resource files. The header contains the database
	 * and Concept Manager application versions. Thus should help to avoid confusion about versioning of resources.
	 * The lines are prepended with the '#' character. Thus, reading applications should accept this character
	 * as a comment indicator.
	 * @param connectionConfiguration Connection configuration to retrieve the database version.
	 * @return A string representing the header.
	 * @throws VersionRetrievalException If the database version cannot be retrieved.
	 * @throws IOException If the application version cannot be read (it is stored in a auto-generated file).
	 */
	default String getResourceHeader(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws VersionRetrievalException, IOException {
		String version = VersioningService.getInstance(connectionConfiguration).getVersion();
		StringBuilder sb = new StringBuilder();
		sb.append("# ").append("Concept database version: ").append(version)
				.append(System.getProperty("line.separator"));
		sb.append("# ").append("Concept Database Manager Application version: ").append(getApplicationVersion())
				.append(System.getProperty("line.separator"));
		return sb.toString();
	}

	default String getApplicationVersion() throws IOException {
		BufferedReader br = new BufferedReader(
				new InputStreamReader(getClass().getResourceAsStream("/concept-db-manager-version.txt")));
		return br.readLine().trim();
	}

	default void writeBase64GzipToFile(String file, String data) throws DataExportException {
		try {
			byte[] decoded = DatatypeConverter.parseBase64Binary(data);
			InputStream is = new ByteArrayInputStream(decoded);
			try {
				is = new GZIPInputStream(is);
			} catch (ZipException e) {
				// don't do anything; so it's not in GZIP format, OK.
			}
			BufferedInputStream bufis = new BufferedInputStream(is);
			byte[] buffer = new byte[2048];
			try (BufferedWriter bw = FileUtilities.getWriterToFile(new File(file))) {
				int bytesRead = -1;
				while ((bytesRead = bufis.read(buffer, 0, buffer.length)) > 0) {
					bw.write(new String(buffer, 0, bytesRead));
				}
			}
		} catch (IOException e) {
			throw new DataExportException(e);
		}
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
     * @param inputData
     * @param decodingConfig
     * @return
     * @throws IOException
     * @throws JSONException
     */
	default String decode(String inputData, HierarchicalConfiguration<ImmutableNode> decodingConfig)
			throws IOException, JSONException {
		Object currentDataState = inputData;
		for (String key : (Iterable<String>) decodingConfig::getKeys) {
			if (key.equalsIgnoreCase("base64") && decodingConfig.getBoolean(key))
				currentDataState = DatatypeConverter.parseBase64Binary(toString(currentDataState));
			if (key.equalsIgnoreCase("gzip") && decodingConfig.getBoolean(key)) {
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

	default String toString(Object o) {
		if (o instanceof String)
			return (String) o;
		else if (o instanceof byte[]) {
			return new String((byte[]) o, Charset.forName("UTF-8"));
		} else
			throw new IllegalArgumentException("The passed object is neither a string nor a byte[]: " + o);
	}
}
