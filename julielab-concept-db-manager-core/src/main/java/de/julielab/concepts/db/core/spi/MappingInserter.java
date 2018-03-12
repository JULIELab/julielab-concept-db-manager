package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.MappingInsertionException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;

import java.util.stream.Stream;

public interface MappingInserter extends DatabaseConnected {
    void insertMappings(Stream<ImportMapping> mappings) throws MappingInsertionException;
}
