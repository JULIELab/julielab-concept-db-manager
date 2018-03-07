package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.DataExportException;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

public abstract class DataExporterBase extends FunctionCallBase implements DataExporter {


	protected static final String CONFKEY_OUTPUT_FILE = "configuration.outputfile";


	protected void writeBase64GzipToFile(String file, String data) throws DataExportException {
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

	protected String decode(String inputData, HierarchicalConfiguration<ImmutableNode> decodingConfig)
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

	private String toString(Object o) {
		if (o instanceof String)
			return (String) o;
		else if (o instanceof byte[]) {
			return new String((byte[]) o, Charset.forName("UTF-8"));
		} else
			throw new IllegalArgumentException("The passed object is neither a string nor a byte[]: " + o);
	}
}
