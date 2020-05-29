package de.julielab.concepts.db.core;

import de.julielab.concepts.util.IncompatibleActionHandlerConnectionException;
import de.julielab.concepts.util.MethodCallException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.java.utilities.ConfigurationUtilities.ws;

public abstract class JavaMethodCallBase extends FunctionCallBase {

    public JavaMethodCallBase(Logger log) {
        super(log);
    }

    /**
     * Retrieves the class and method names via the configuration keys CONFKEY_CLASS_NAME and
     * CONFKEY_METHOD_NAME from the configuration, respectively.
     *
     * @param <T>                    The return value of the called method.
     * @param parameterConfiguration The subconfiguration immediately containing the parameters.
     * @param dbms                The graph database to apply the method call on. The graph database must be the first argument of the method.
     * @return The return value of the called method.
     * @throws MethodCallException If any part of the processing of calling the method fails.
     */
    protected <T> T callInstanceMethod(HierarchicalConfiguration<ImmutableNode> parameterConfiguration, DatabaseManagementService dbms) throws MethodCallException, IncompatibleActionHandlerConnectionException {
        String className = parameterConfiguration.getString(CLASS);
        String methodName = parameterConfiguration.getString(METHOD);
        if (className == null || methodName == null)
            throw new IncompatibleActionHandlerConnectionException("No class and/or no method name for a Java method call given. Skipping.");
        try {
            Map<String, Parameter> parsedParameters = parseParameters(parameterConfiguration.configurationAt(PARAMETERS));
            Object classInstance = Class.forName(className).getDeclaredConstructor(DatabaseManagementService.class).newInstance(dbms);
            Class<?>[] parameterTypes = parsedParameters.values().stream().map(Parameter::getType)
                    .toArray(i -> new Class<?>[i]);
            checkTypesForNull(parameterTypes, parsedParameters);
            Method exporterMethod = classInstance.getClass().getDeclaredMethod(methodName, parameterTypes);
            Stream<Object> paramValueStream = parsedParameters.values().stream().map(Parameter::getValue);
            Object[] values = paramValueStream.toArray(i -> new Object[i]);
            T result = (T) exporterMethod.invoke(classInstance, values);
            return result;
        } catch (InstantiationException | IllegalAccessException |
                ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            throw new MethodCallException(e);
        }
    }

    private void checkTypesForNull(Class<?>[] parameterTypes, Map<String, Parameter> parsedParameters) throws MethodCallException {
        Iterator<Parameter> parametersIt = parsedParameters.values().iterator();
        for (int i = 0; i < parameterTypes.length; i++) {
            Parameter parameter = parametersIt.next();
            Class<?> parameterClass = parameterTypes[i];
            if (parameterClass == null)
                throw new MethodCallException(
                        "A multi-valued parameter did not specify its element type. The parameter is: "
                                + parameter);
        }
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, REQUEST, CLASS), "");
        template.addProperty(slash(basePath, REQUEST, METHOD), "");
        template.addProperty(slash(basePath, REQUEST, PARAMETERS, "parametername"), "value");
        template.addProperty(ws(slash(basePath, REQUEST, PARAMETERS, "parametername"), "@parametername"), "optional: parameter name");
        template.addProperty(ws(slash(basePath, REQUEST, PARAMETERS, "parametername"), "@paramertype"), "mandatory: parameter type");
        template.addProperty(slash(basePath, REQUEST, PARAMETERS, "arrayparameter", "arrayitem"), Arrays.asList("value1", "value2"));
        template.addProperty(ws(slash(basePath, REQUEST, PARAMETERS, "arrayparameter"), "@elementtype"), "mandatory: array element type");
    }
}
