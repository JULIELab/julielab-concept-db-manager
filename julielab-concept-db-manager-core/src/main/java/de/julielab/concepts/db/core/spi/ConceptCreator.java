package de.julielab.concepts.db.core.spi;

import static java.util.stream.Collectors.joining;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptCreator {
	public static final String LS = System.getProperty("line.separator");

	Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
			throws ConceptCreationException, FacetCreationException;

	/**
	 * Checks if a concrete service provider implementation of this service provider
	 * interface matches the given name. The naming contract enforces that each
	 * provider must accept its own qualified class name as provider name. Other
	 * names are optional and might facilitate the usage of the provider in the
	 * configuration file.
	 * 
	 * @param providername
	 *            A name to check whether it matches the name of a provider.
	 * @return True, if the given name matches the provider's, false otherwise.
	 */
	boolean hasName(String providername);
	
	default void checkParameters(HierarchicalConfiguration<ImmutableNode> importConfig, String... parameters) throws ConceptCreationException {
		List<String> parameterNotFound = new ArrayList<>();
		for (String parameter : parameters) {
			if (importConfig.getProperty(parameter) == null)
				parameterNotFound.add(parameter);
		}
		if (!parameterNotFound.isEmpty())
			throw new ConceptCreationException("The following required parameters are not set in the configuration:"+LS
					+ parameterNotFound.stream().collect(joining(LS)));
	}
	
	default void checkFilesExist(HierarchicalConfiguration<ImmutableNode> importConfig, String... parameters) throws ConceptCreationException {
		checkParameters(importConfig, parameters);
		List<String> parameterNotFound = new ArrayList<>();
		for (String parameter : parameters) {
			if (!new File(importConfig.getString(parameter)).exists())
				parameterNotFound.add(parameter);
		}
		if (!parameterNotFound.isEmpty())
			throw new ConceptCreationException("The following required files given by the configuration do not exist: "+LS
					+ parameterNotFound.stream().collect(joining(LS)));
	}
}
