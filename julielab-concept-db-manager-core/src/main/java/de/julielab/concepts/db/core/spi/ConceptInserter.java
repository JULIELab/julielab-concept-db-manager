package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.annotation.Nullable;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

/**
 * Concept inserters require an instance of  {@link ImportConcepts} which is delivered by the {@link de.julielab.concepts.db.core.services.ConceptCreationService}.
 * Concept insertion is a bit special within the Concept Manager infsofar that it does not represent a point of extension.
 * Concept creation, facet creation and data export may all be extended by custom classes. This is not necessary for
 * insertion because the input format is always the same: An instance of {@link ImportConcepts}. There is a fixed
 * set of service providers for different connection types but no further flexibility is required. This is why
 * the interface does not specify the hasName() method common to the other interfaces - its implementations are only
 * identified with regard to their supported connection, not by their processing logic.
 */
public interface ConceptInserter extends DatabaseConnected {
	void insertConcepts(HierarchicalConfiguration<ImmutableNode> importConfiguration, ImportConcepts concepts) throws ConceptInsertionException;

	/**
	 * Used to set or override {@link ImportOptions} for all concept inserters.
	 * @param importConfiguration The import configuration.
	 * @param optionsFromConceptCreator The import options as retrieved from the concept creator.
	 * @return The adapted or newly created import options.
	 */
	default ImportOptions setGlobalOptions(HierarchicalConfiguration<ImmutableNode> importConfiguration, @Nullable ImportOptions optionsFromConceptCreator) {
		ImportOptions retOptions = optionsFromConceptCreator != null ? optionsFromConceptCreator : new ImportOptions();
		final Boolean merge = importConfiguration.getBoolean(slash(IMPORT_OPTIONS, MERGE));
		final Boolean overridePreferredName = importConfiguration.getBoolean(slash(IMPORT_OPTIONS, OVERRIDE_PREFERRED_NAME));

		if (merge != null)
			retOptions.merge = merge;
		if (overridePreferredName != null)
			retOptions.overridePreferredName = overridePreferredName;


		return retOptions;
	}
}
