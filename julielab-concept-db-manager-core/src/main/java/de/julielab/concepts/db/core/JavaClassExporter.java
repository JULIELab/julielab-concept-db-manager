package de.julielab.concepts.db.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;

import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.DataExportException;

public class JavaClassExporter extends DataExporterBase {

	private static final String CONFKEY_CLASS_NAME = "configuration.class";
	private static final String CONFKEY_METHOD_NAME = "configuration.method";
	private GraphDatabaseService graphDb;

	@Override
	public void exportData(HierarchicalConfiguration<ImmutableNode> exportConfig)
			throws ConceptDatabaseConnectionException, DataExportException {
		String className = exportConfig.getString(CONFKEY_CLASS_NAME);
		String methodName = exportConfig.getString(CONFKEY_METHOD_NAME);
		String outputFile = exportConfig.getString(CONFKEY_OUTPUT_FILE);
		Map<String, Parameter> parsedParameters = parseParameters(exportConfig.configurationAt(CONFKEY_PARAMETERS));
		try {
			Object exporterInstance = Class.forName(className).newInstance();
			Class<?>[] parameterTypes = parsedParameters.values().stream().map(Parameter::getType)
					.toArray(i -> new Class<?>[i]);
			checkTypesForNull(parameterTypes, parsedParameters);
			Method exporterMethod = exporterInstance.getClass().getDeclaredMethod(methodName, parameterTypes);
			if (exporterMethod.getReturnType() != String.class)
				throw new DataExportException("The method " + methodName + " does return an object of type "
						+ exporterMethod.getReturnType() + " but " + String.class.getCanonicalName() + " is requried.");
			Stream<Object> paramValueStream = parsedParameters.values().stream().map(Parameter::getValue);
			// The database instance must be the first argument.
			paramValueStream = Stream.concat(Stream.of(graphDb), paramValueStream);
			Object[] values = paramValueStream.toArray(i -> new Object[i]);
			String result = (String) exporterMethod.invoke(exporterInstance, values);
			writeBase64GzipToFile(outputFile, result);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
				| SecurityException | IllegalArgumentException | InvocationTargetException e) {
			throw new DataExportException(e);
		}
	}

	private void checkTypesForNull(Class<?>[] parameterTypes, Map<String, Parameter> parsedParameters) throws DataExportException {
		Iterator<Parameter> parametersIt = parsedParameters.values().iterator();
		for (int i = 0; i < parameterTypes.length; i++) {
			Parameter parameter = parametersIt.next();
			Class<?> parameterClass = parameterTypes[i];
			if (parameterClass == null)
				throw new DataExportException(
						"A multi-valued parameter did not specify its element type. The parameter is: "
								+ parameter);
		}
	}

	@Override
	public boolean hasName(String providerName) {
		return providerName.equalsIgnoreCase("javaclassexporter") || providerName.equals(getClass().getCanonicalName());
	}

	@Override
	public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws ConceptDatabaseConnectionException {
		graphDb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
	}

}
