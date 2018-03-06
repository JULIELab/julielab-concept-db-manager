package de.julielab.concepts.db.core.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

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
}
