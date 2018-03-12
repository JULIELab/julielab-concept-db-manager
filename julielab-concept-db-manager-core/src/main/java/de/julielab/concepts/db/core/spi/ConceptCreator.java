package de.julielab.concepts.db.core.spi;

import java.util.stream.Stream;

import de.julielab.jssf.commons.spi.ExtensionPoint;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public interface ConceptCreator extends ExtensionPoint, ParameterExposing {

	Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
			throws ConceptCreationException, FacetCreationException;

}
