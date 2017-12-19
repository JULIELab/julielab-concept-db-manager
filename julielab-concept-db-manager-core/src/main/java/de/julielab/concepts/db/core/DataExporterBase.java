package de.julielab.concepts.db.core;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.spi.DataExporter;

public abstract class DataExporterBase implements DataExporter {

	protected class Parameter {
		private String name;
		private Object value;
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
			return "Parameter [name=" + name + ", value=" + value + ", isList=" + isList + "]";
		}
		
		
	}
	
	protected Map<String, Parameter> parseParameters(HierarchicalConfiguration<ImmutableNode> parameterConfiguration) {
		Map<String, Parameter> parameterMap = new HashMap<>();
		// First pass through the configuration: Identify parameters, their correct
		// names and if they are multi-valued.
		for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
			if (!key.contains(".") && !key.contains("@parametername") && !key.contains("[")) {
				// e.g. conceptlabel
				Parameter parameter = parameterMap.computeIfAbsent(key, name -> new Parameter());
				parameter.setNameIfAbsent(key);
				parameterMap.put(key, parameter);
			} else if (!key.contains(".") && key.contains("@parametername")) {
				// e.g. conceptlabel[@parametername]
				String parameterElementName = key.substring(0, key.indexOf('['));
				Parameter parameter = parameterMap.computeIfAbsent(parameterElementName, name -> new Parameter());
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

}
