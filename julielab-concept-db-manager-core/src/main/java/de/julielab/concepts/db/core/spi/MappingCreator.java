package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.concepts.util.MappingCreationException;
import de.julielab.jssf.commons.spi.ExtensionPoint;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.stream.Stream;

public interface MappingCreator extends ExtensionPoint, DatabaseConnected, ParameterExposing {
    Stream<ImportMapping> createMappings(HierarchicalConfiguration<ImmutableNode> importConfig)
            throws MappingCreationException;
}
