package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.stream.Stream;

public interface MappingInserter extends DatabaseConnected {
    void insertMappings(HierarchicalConfiguration<ImmutableNode> importConfiguration, Stream<ImportMapping> mappings) throws MappingInsertionException;
}
