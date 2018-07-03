package de.julielab.concepts.db.core;

import com.google.gson.Gson;
import de.julielab.concepts.util.MethodCallException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;

/**
 * This class contains the {@link Parameter} type and methods to parse parameters from the configuration
 * file. It should be extended by all classes reading function or method parameters from the
 * configuration to then call the function or method.
 */
public abstract class FunctionCallBase implements ParameterExposing {

    public static final String CONFKEY_PARAMETERS = dot(CONFIGURATION, PARAMETERS);
    public static final String CONFKEY_CLASS_NAME = dot(CONFIGURATION, CLASS);
    public static final String CONFKEY_METHOD_NAME = dot(CONFIGURATION, METHOD);
    protected Logger log;

    public FunctionCallBase(Logger log) {
        this.log = log;
    }

    protected Map<String, Parameter> parseParameters(HierarchicalConfiguration<ImmutableNode> parameterConfiguration)
            throws MethodCallException {

        Map<String, Parameter> parameterMap = new LinkedHashMap<>();
        ImmutableNode configTree = parameterConfiguration.getNodeModel().getInMemoryRepresentation();
        try {
            for (ImmutableNode parameterNode : configTree.getChildren()) {
                Map<String, Object> attributes = parameterNode.getAttributes();
                String name = (String) attributes.get("parametername");
                if (name == null)
                    name = parameterNode.getNodeName();
                String type = (String) attributes.get("parametertype");
                Boolean tojson = Boolean.parseBoolean(Optional.ofNullable((String) attributes.get("tojson")).orElse("false"));
                String elementtype = (String) attributes.get("elementtype");
                boolean islist = !parameterNode.getChildren().isEmpty();
                // Will be null for array-valued parameters
                Object value = parameterNode.getValue();

                Parameter parameter = new Parameter();
                parameter.setName(name);
                if (type != null)
                    parameter.setType(Class.forName(type));
                parameter.setConvertToJson(tojson);
                if (elementtype != null)
                    parameter.setElementType(Class.forName(elementtype));
                parameter.setIsList(islist);
                parameter.setValue(value);

                List<Object> arrayitems = new ArrayList<>();
                for (ImmutableNode listitem : parameterNode.getChildren()) {
                    arrayitems.add(listitem.getValue());
                }
                if (!arrayitems.isEmpty())
                    parameter.setValue(arrayitems);

                parameterMap.put(name, parameter);

            }
        } catch (ClassNotFoundException e) {
            throw new MethodCallException(e);
        }
        return parameterMap;
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

        public void setValue(Object value) {
            this.value = value;
        }

        public Object getRequestValue() {
            return convertToJson ? gson.toJson(getValue()) : getValue();
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

        public Class<?> getElementType() {
            return this.elementType;
        }

        public void setElementType(Class<?> elementype) {
            this.elementType = elementype;
        }
    }
}
