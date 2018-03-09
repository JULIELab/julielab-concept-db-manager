package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.db.core.services.HttpConnectionService;
import de.julielab.concepts.db.core.services.NetworkConnectionCredentials;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_ENDPOINT;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.CONFKEY_PLUGIN_NAME;
import static de.julielab.concepts.db.core.ServerPluginConnectionConstants.SERVER_PLUGIN_PATH_FMT;

/**
 * This class contains the {@link Parameter} type and methods to parse parameters from the configuration
 * file. It should be extended by all classes reading function or method parameters from the
 * configuration to then call the function or method.
 */
public abstract class FunctionCallBase {

    public static final String CONFKEY_PARAMETERS = dot(CONFIGURATION, PARAMETERS);
    public static final String CONFKEY_CLASS_NAME = dot(CONFIGURATION, ConfigurationConstants.CLASS);
    public static final String CONFKEY_METHOD_NAME = dot(CONFIGURATION, METHOD);
    protected Logger log;

    public FunctionCallBase(Logger log) {
        this.log = log;
    }

    protected class Parameter {
        private String name;
        private Object value;
        private Class<?> type;
        private boolean isList;
        private Gson gson = new Gson();
        private boolean convertToJson;
        private Class<?> elementType;

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

        public void setElementType(Class<?> elementype) {
            this.elementType = elementype;
        }

        public Class<?> getElementType() {
            return this.elementType;
        }
    }

    protected Map<String, Parameter> parseParameters(HierarchicalConfiguration<ImmutableNode> parameterConfiguration)
            throws MethodCallException {

        Map<String, Parameter> parameterMap = new LinkedHashMap<>();

        // First pass through the configuration: Identify parameters, their correct
        // names and if they are multi-valued.
        try {
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
                        Parameter parameter = parameterMap.computeIfAbsent(parameterElementName,
                                name -> new Parameter());
                        if (key.contains("@parametername"))
                            parameter.setName(parameterConfiguration.getString(key));
                        if (key.contains("@elementtype"))
                            parameter.setElementType(Class.forName(parameterConfiguration.getString(key)));
                        parameterMap.put(parameterElementName, parameter);
                    } else if (key.contains(".")) {
                        // e.g. facetlabels.facetlabel
                        String parameterElementName = key.substring(0, key.indexOf('.'));
                        Parameter parameter = parameterMap.computeIfAbsent(parameterElementName,
                                name -> new Parameter());
                        parameter.setNameIfAbsent(parameterElementName);
                        parameter.setIsList(true);
                    }
                }
            }

            Matcher elementNameMatcher = Pattern.compile("([^.\\[]+).*").matcher("");
            // Second pass: Fill the values and properties into the parameters
            for (String key : (Iterable<String>) parameterConfiguration::getKeys) {
                if (!StringUtils.isBlank(key)) {
                    if (elementNameMatcher.reset(key).matches()) {
                        String parameterElementName = elementNameMatcher.group(1);
                        Parameter parameter = parameterMap.get(parameterElementName);
                        if (parameter == null)
                            throw new IllegalStateException(
                                    "The regular expression for the identification of the XML configuration element is faulty: It extracted the element name \""
                                            + parameterElementName + "\" for the parameter key \"" + key + "\".");
                        if ((parameter.isList() && key.contains("."))) {
                            Class<?> elementType = parameter.getElementType();
                            if (elementType != null)
                                parameter.setValue(parameterConfiguration.getList(elementType, key));
                            else
                                parameter.setValue(parameterConfiguration.getList(key));
                        }
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
            throw new MethodCallException(e);
        }
        return parameterMap;
    }
}
