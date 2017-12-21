package de.julielab.concepts.db.core;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;

import com.google.gson.Gson;

import de.julielab.concepts.db.core.spi.DataExporter;
import de.julielab.concepts.util.DataExportException;
import de.julielab.java.utilities.FileUtilities;

public abstract class DataExporterBase implements DataExporter {

	protected static final String CONFKEY_PARAMETERS = "configuration.parameters";
	protected static final String CONFKEY_OUTPUT_FILE = "configuration.outputfile";

	protected class Parameter {
		private String name;
		private Object value;
		private Class<?> type;
		private boolean isList;
		private Gson gson = new Gson();
		private boolean convertToJson;

		public boolean isList() {
			return isList;
		}

		public void setNameIfAbsent(String name) {
			if (this.name == null)
				this.name = name;
		}

		public void setIsList(boolean isList) {
			this.isList = isList;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Object getValue() {
			return value;
		}

		public Object getRequestValue() {
			return convertToJson ? gson.toJson(getValue()) : getValue();
		}

		public void setValue(Object value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "Parameter [name=" + name + ", value=" + value + ", type=" + type + ", isList=" + isList + "]";
		}

		public Class<?> getType() {
			return type;
		}

		public void setType(Class<?> type) {
			this.type = type;
		}

		public void setConvertToJson(boolean convertToJson) {
			this.convertToJson = convertToJson;
		}

		public boolean convertToJson() {
			return convertToJson;
		}

	}

	protected Map<String, Parameter> parseParameters(HierarchicalConfiguration<ImmutableNode> parameterConfiguration)
			throws DataExportException {

		Map<String, Parameter> parameterMap = new LinkedHashMap<>();

		// First pass through the configuration: Identify parameters, their correct
		// names and if they are multi-valued.
		for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
			if (!StringUtils.isBlank(key)) {
				if (!key.contains(".") && !key.contains("@parametername") && !key.contains("[")) {
					// e.g. conceptlabel
					Parameter parameter = parameterMap.computeIfAbsent(key, name -> new Parameter());
					parameter.setNameIfAbsent(key);
					parameterMap.put(key, parameter);
				} else if (!key.contains(".") && (key.contains("@"))) {
					String parameterElementName = key.substring(0, key.indexOf('['));
					// e.g. conceptlabel[@parametername]
					Parameter parameter = parameterMap.computeIfAbsent(parameterElementName, name -> new Parameter());
					if (key.contains("@parametername"))
						parameter.setName(parameterConfiguration.getString(key));
					parameterMap.put(parameterElementName, parameter);
				} else if (key.contains(".")) {
					// e.g. facetlabels.facetlabel
					String parameterElementName = key.substring(0, key.indexOf('.'));
					Parameter parameter = parameterMap.computeIfAbsent(parameterElementName, name -> new Parameter());
					parameter.setNameIfAbsent(parameterElementName);
					parameter.setIsList(true);
				}
			}
		}

		try {
			Matcher elementNameMatcher = Pattern.compile("([^.\\[]+).*").matcher("");
			// Second pass: Fill the values and properties into the parameters
			for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
				if (!StringUtils.isBlank(key)) {
					if (elementNameMatcher.reset(key).matches()) {
						String parameterElementName = elementNameMatcher.group(1);
						Parameter parameter = parameterMap.get(parameterElementName);
						if (parameter == null)
							throw new IllegalStateException(
									"The regular expression for the identification of the XML configuration element is faulty: It extracted the element name \"" + parameterElementName + "\" for the parameter key \"" + key + "\".");
						if ((parameter.isList() && key.contains(".")))
							parameter.setValue(parameterConfiguration.getList(key));
						if (!parameter.isList() && !key.contains(".") && !key.contains("@"))
							parameter.setValue(parameterConfiguration.getString(key));
						if (!key.contains(".") && key.contains("@")) {
							if (key.contains("@parametertype"))
								parameter.setType(Class.forName(parameterConfiguration.getString(key)));
							if (key.contains("@tojson"))
								parameter.setConvertToJson(parameterConfiguration.getBoolean(key));
						}

					} else
						throw new IllegalStateException(
								"The regular expression for the identification of the XML configuration element is faulty: It did not find any match on the configuration key \""
										+ key + "\"");
				}
			}
		} catch (ClassNotFoundException e) {
			throw new DataExportException(e);
		}
		return parameterMap;
	}

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
