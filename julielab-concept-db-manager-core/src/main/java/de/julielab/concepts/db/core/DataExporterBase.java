package de.julielab.concepts.db.core;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

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

	}

	protected Map<String, Parameter> parseParameters(HierarchicalConfiguration<ImmutableNode> parameterConfiguration) throws DataExportException {
		
		Map<String, Parameter> parameterMap = new LinkedHashMap<>();
		try {
			// First pass through the configuration: Identify parameters, their correct
			// names and if they are multi-valued.
			for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
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
					if (key.contains("@parametertype"))
						parameter.setType(Class.forName(parameterConfiguration.getString(key)));
					parameterMap.put(parameterElementName, parameter);
				} else if (key.contains(".")) {
					// e.g. facetlabels.facetlabel
					String parameterElementName = key.substring(0, key.indexOf('.'));
					Parameter parameter = parameterMap.computeIfAbsent(parameterElementName, name -> new Parameter());
					parameter.setNameIfAbsent(parameterElementName);
					parameter.setIsList(true);
				}
			}
		} catch (ClassNotFoundException e) {
			throw new DataExportException(e);
		}

		Matcher elementNameMatcher = Pattern.compile("([^\\p{P}]+).*").matcher("");
		// Second pass: Fill the values into the parameters
		for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
			if (elementNameMatcher.reset(key).matches()) {
				String parameterElementName = elementNameMatcher.group(1);
				Parameter parameter = parameterMap.get(parameterElementName);
				if (parameter == null)
					throw new IllegalStateException(
							"The regular expression for the identification of the XML configuration element is faulty: It did not find the capture group of the configuration key \""
									+ key + "\"");
				if ((parameter.isList() && key.contains(".")))
					parameter.setValue(parameterConfiguration.getList(key));
				if (!parameter.isList() && !key.contains(".") && !key.contains("@"))
					parameter.setValue(parameterConfiguration.getString(key));
			} else
				throw new IllegalStateException(
						"The regular expression for the identification of the XML configuration element is faulty: It did not find any match on the configuration key \""
								+ key + "\"");
		}
		return parameterMap;
	}

	protected void writeBase64GzipToFile(String file, String data) throws DataExportException {
		try {
			byte[] decoded = DatatypeConverter.parseBase64Binary(data);
			InputStream is = new ByteArrayInputStream(decoded);
			is = new GZIPInputStream(is);
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

}
